import yaml
import numpy as np
import pandas as pd
from DataManager import *

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
    subsample = config_psql["subsample_time"]

    electron_lifetime = DataManager(PsqlDB.get_purity_monitor_data(tablename=config_psql["purity_mon_table"]))
    cryostat_pressure = DataManager(PsqlDB.get_cryostat_data(table_prefix=config_psql["cryo_table_prefix"], tagid=config_psql["cryopress_tagid"]))
    LAr_level = DataManager(PsqlDB.get_cryostat_data(table_prefix=config_psql["cryo_table_prefix"], tagid=config_psql["LAr_level_tagid"]))
    measurements = {
        "electron_lifetime": electron_lifetime,
        "cryostat_pressure": cryostat_pressure,
        "LAr_level": LAr_level
    }
    for varname, meas in measurements.items(): dump(meas.format(source="psql", variables=[varname], subsample_interval=config_psql["subsample_time"]), filename= PsqlDB.make_filename(varname))

def get_gizmo_ground_tag(influxDB):
    database="gizmo"
    measurement="resistance"
    ground_impedance=config_influx["ground_impedance"]
    ground_impedance_err=config_influx["ground_impedance_err"]

    variables = influxDB.fetch_measurement_fields(database, measurement)
    result_data = DataManager(influxDB.fetch_measurement_data(database, measurement, variables))
    formatted_data = result_data.format(source="influx", variables=variables, subsample_interval=None)

    tag = "good"
    bad_ground_values = []

    for entry in formatted_data:
        if entry["resistance"] < ground_impedance-ground_impedance_err or entry["resistance"] > ground_impedance+ground_impedance_err: bad_ground_values.append(entry)

    bag_ground_percent = len(bad_ground_values)*100./len(formatted_data)
    if bad_ground_values: print(f"WARNING: Bad grounding detected at {len(bad_ground_values)}({round(bag_ground_percent,2)}%) instances at these times: {bad_ground_values}")
    if bag_ground_percent >= config_influx["bad_ground_percent_threshold"]: tag = "bad"

    return tag, len(bad_ground_values)

def get_LAr_level_tag(PsqlDB):
    LAr_level = DataManager(PsqlDB.get_cryostat_data(table_prefix=config_psql["cryo_table_prefix"], tagid=config_psql["LAr_level_tagid"]))
    formatted_data = LAr_level.format(source="psql", variables=["LAr_level"], subsample_interval=None)
    bad_level_values = []
    tag = "good"
    for entry in formatted_data:
        if entry["LAr_level"] < config_psql["good_LAr_level"]:
            bad_level_values.append(entry)
            if(tag != "bad"): tag="bad"
    if bad_level_values:
        print(f"WARNING: Bad LAr level detected at {len(bad_level_values)}({round(len(bad_level_values)*100./len(formatted_data), 2)}%) instances at these times: {bad_level_values}")

    return tag

def calculate_effective_shell_resistances(influxDB, V_set=0.0):
    database="HVmonitoring"
    measurement="Raspi"
    variables = ["CH0", "CH1", "CH2", "CH3"]

    result_data = DataManager(influxDB.fetch_measurement_data(database, measurement, variables))
    formatted_data = result_data.format(source="influx", variables=variables, subsample_interval=None)

    effective_shell_resistances = np.zeros(4)
    pick_off_voltages = np.zeros(4)
    R_pick=config_influx["pick_off_resistance"]
    for i, var in enumerate(variables):
        V_pick = get_mean(formatted_data, var)
        if V_pick == 0.0:
            print(f"WARNING: The pick-off voltage for {var} is 0.0!")
            continue
        pick_off_voltages[i] = V_pick
        R_eff = R_pick*((V_set/V_pick) -1)
        effective_shell_resistances[i] = R_eff

    return pick_off_voltages, effective_shell_resistances

def calculate_electric_fields(influxDB):
    database="HVmonitoring"
    measurement="SPELLMAN_HV"
    variables = ["Voltage"]
    electric_fields =  np.zeros(4)
    pick_off_voltages = np.zeros(4)
    mean_voltage = 0.0

    result_data = DataManager(influxDB.fetch_measurement_data(database, measurement, variables))
    formatted_data = result_data.format(source="influx", variables=variables, subsample_interval=None)

    if not formatted_data:
        return mean_voltage, pick_off_voltages, electric_fields

    mean_voltage = get_mean(formatted_data, variables[0])

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
        electron_lifetime = PsqlDB.get_purity_monitor_data(tablename=config_psql["purity_mon_table"], last_value=True)
        set_voltage, pick_off_voltages, electric_fields = calculate_electric_fields(influxDB=influxDB)

        data =\
        {
        "Gizmo grounding": {
            "Overall grounding": ground_tag,
            "Number of shorts": shorts_num
            },
        "Liquid Argon level": LAr_tag,
        "Purity monitor": {
            "Last timestamp": electron_lifetime[0].isoformat(),
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
