import yaml
import numpy as np
import pandas as pd
from DataManager import *

chicago_tz = pytz.timezone('America/Chicago')

config_influx = {}
config_psql = {}

def load_config(config_file):
    global config_influx, config_psql
    config=None
    with open(config_file, "r") as yaml_file:
        config=yaml.safe_load(yaml_file)

    config_influx=config["influxdb"]
    config_psql=config["psql"]

def get_mean(data, varname):
    df = pd.DataFrame(data)
    return df[varname].mean()

def influx_blind_dump(influxDB):
    data_dict = config_influx.get("influx_SC_data_dict", {})
    for database, (measurements, variables) in data_dict.items():
        if not variables:
            variables = [influxDB.fetch_measurement_fields(database, measurement) for measurement in measurements]
        for measurement, measurement_variables in zip(measurements, variables):
            result_data = DataManager(influxDB.fetch_measurement_data(database, measurement, measurement_variables))
            dump(result_data.format(source="influx", variables=measurement_variables, subsample_interval=config_influx["subsample_time"]), filename=influxDB.make_filename(database, measurement))

def psql_blind_dump(PsqlDB):
    purity_mon_dict = config_psql.get("purity_mon_variables", {})
    varnames = list(purity_mon_dict.keys())
    variables = list(purity_mon_dict.values())
    purity_monitor = DataManager(PsqlDB.get_purity_monitor_data(tablename=config_psql["purity_mon_table"], variables=variables))
    dump(purity_monitor.format(source="psql", variables=varnames, subsample_interval=config_psql["subsample_time"]), filename= PsqlDB.make_filename("purity_monitor"))

    cryostat_dict = config_psql.get("cryostat_tag_dict", {})
    for varname, tagid in cryostat_dict.items():
        data = DataManager(PsqlDB.get_cryostat_data(table_prefix=config_psql["cryo_table_prefix"], variable=varname, tagid=tagid))
        dump(data.format(source="psql", variables=[varname], subsample_interval=config_psql["subsample_time"]), filename= PsqlDB.make_filename(varname))

def get_influx_db_meas_vars(meas_name):
    database, measurement, variables = config_influx.get("influx_SC_special_dict", {}).get(meas_name, [None, None, None])
    if not database and measurement and variables:
        raise ValueError(f"The given measurement name {meas_name} is not in the dict. Check influx_SC_special_dict in config/parameters.yaml")
    return database, measurement, variables

def get_gizmo_ground_tag(influxDB):
    database, measurement, variables = get_influx_db_meas_vars("ground_impedance")
    ground_impedance=config_influx["ground_impedance"]
    ground_impedance_err=config_influx["ground_impedance_err"]

    data = DataManager(influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=None)

    tag = "good"
    bad_ground_values = []

    for entry in data:
        if entry["resistance"] < ground_impedance-ground_impedance_err or entry["resistance"] > ground_impedance+ground_impedance_err: bad_ground_values.append(entry)

    bag_ground_percent = len(bad_ground_values)*100./len(data)
    if bad_ground_values: print(f"WARNING: Bad grounding detected at {len(bad_ground_values)}({round(bag_ground_percent,2)}%) instances at these times: {bad_ground_values}")
    if bag_ground_percent >= config_influx["bad_ground_percent_threshold"]: tag = "bad"

    return tag, len(bad_ground_values)

def get_LAr_level_tag(PsqlDB):
    data = DataManager(PsqlDB.get_cryostat_data(table_prefix=config_psql["cryo_table_prefix"], variable="LAr_level", tagid=config_psql["cryostat_tag_dict"]["LAr_level"])).format(source="psql", variables=["LAr_level"], subsample_interval=None)
    good_LAr_level = config_psql["good_LAr_level"]
    bad_level_values = []
    tag = "good"

    for entry in data:
        if entry["LAr_level"] < good_LAr_level or entry["LAr_level"] > good_LAr_level:
            bad_level_values.append(entry)
            if(tag != "bad"): tag="bad"

    if bad_level_values:
        print(f"WARNING: Bad LAr level detected at {len(bad_level_values)}({round(len(bad_level_values)*100./len(data), 2)}%) instances at these times: {bad_level_values}")

    return tag

def calculate_effective_shell_resistances(influxDB, V_set=0.0):
    database, measurement, variables = get_influx_db_meas_vars("pick_off_voltages")
    effective_shell_resistances = np.zeros(4)
    pick_off_voltages = np.zeros(4)

    data = DataManager(influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=None)

    if not data:
        return  pick_off_voltages, effective_shell_resistances

    R_pick=config_influx["pick_off_resistance"]
    for i, var in enumerate(variables):
        V_pick = get_mean(data, var)
        if V_pick == 0.0:
            print(f"WARNING: The pick-off voltage for {var} is 0.0!")
            continue
        pick_off_voltages[i] = V_pick
        R_eff = R_pick*((V_set/V_pick) -1)
        effective_shell_resistances[i] = R_eff

    return pick_off_voltages, effective_shell_resistances

def calculate_electric_fields(influxDB):
    database, measurement, variables = get_influx_db_meas_vars("set_voltage")
    electric_fields =  np.zeros(4)
    pick_off_voltages = np.zeros(4)
    mean_voltage = 0.0

    data = DataManager(influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=None)

    if not data:
        return mean_voltage, pick_off_voltages, electric_fields

    mean_voltage = get_mean(data, variables[0])

    if mean_voltage == 0.0:
        print("WARNING: The set voltage from Spellman HV is 0.0!")
        return mean_voltage, pick_off_voltages, electric_fields

    pick_off_voltages, effective_shell_resistances= calculate_effective_shell_resistances(influxDB=influxDB, V_set=mean_voltage)
    R_pick=config_influx["pick_off_resistance"]
    drift_dist=config_influx["drift_dist"]

    for i, R_eff in enumerate(eff_resistance):
        E = mean_voltage*(1-(R_pick/(R_pick+R_eff)))/drift_dist
        electric_fields[i] = E

    return mean_voltage, pick_off_voltages, electric_fields

def dump_SC_data(influxDB, PsqlDB, config_file, json_filename="", dump_all_data=False):

    load_config(config_file)

    if(dump_all_data):
        influx_blind_dump(influxDB=influxDB)
        psql_blind_dump(PsqlDB=PsqlDB)
    else:
        ground_tag, shorts_num = get_gizmo_ground_tag(influxDB=influxDB)
        LAr_tag = get_LAr_level_tag(PsqlDB=PsqlDB)
        electron_lifetime = PsqlDB.get_purity_monitor_data(tablename=config_psql["purity_mon_table"], variables=["prm_lifetime"], last_value=True)
        set_voltage, pick_off_voltages, electric_fields = calculate_electric_fields(influxDB=influxDB)

        data =\
        {
        "Gizmo grounding": {
            "Overall grounding": ground_tag,
            "Number of shorts": shorts_num
            },
        "Liquid Argon level": LAr_tag,
        "Purity monitor": {
            "Last timestamp": pd.to_datetime(electron_lifetime[0], utc=True).tz_convert(chicago_tz).isoformat(),
            "Electron lifetime (s)": electron_lifetime[1]
            },
        "Mean Spellman set voltage (kV)": set_voltage,
        "Mean pick-off voltages (kV)": {
            "Module 0": pick_off_voltages[0],
            "Module 1": pick_off_voltages[1],
            "Module 2": pick_off_voltages[2],
            "Module 3": pick_off_voltages[3],
            },
        "Electric fields (kV/m)": {
            "Module 0": electric_fields[0],
            "Module 1": electric_fields[1],
            "Module 2": electric_fields[2],
            "Module 3": electric_fields[3],
            }
        }

        dump(data, filename=json_filename)
