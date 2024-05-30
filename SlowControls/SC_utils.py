import yaml
import json
import numpy as np

config_influx = {}
config_psql = {}

def load_config(config_file):
    global config_influx, config_psql
    config=None
    with open(config_file, "r") as yaml_file:
        config=yaml.safe_load(yaml_file)

    config_influx=config["influxdb"]
    config_psql=config["psql"]

def get_mean_measurement(data, varname):
    sum = 0.0
    for entry in data:
        sum += entry[varname]
    return sum/len(data)

def influx_blind_dump(influxDB):
    data_dict = config_influx.get("influx_SC_data_dict", {})
    for database, (measurements, variables) in data_dict.items():
        if not variables:
            variables = [influxDB.fetch_measurement_fields(database, measurement) for measurement in measurements]
        for measurement, measurement_variables in zip(measurements, variables):
            result_data = influxDB.fetch_measurement_data(database, measurement, measurement_variables)
            formatted_data = influxDB.get_subsampled_formatted_data(database, measurement, measurement_variables, result_data, subsample=config_influx["subsample_time"])
            influxDB.dump_to_json(formatted_data, database, measurement)

def psql_blind_dump(PsqlDB):
    subsample = config_psql["subsample_time"]
    electron_lifetime = PsqlDB.get_purity_monitor_data(tablename=config_psql["purity_mon_table"])
    subsampled_electron_lifetime = PsqlDB.get_subsampled_formatted_data(electron_lifetime,"electron_lifetime", subsample=subsample)
    PsqlDB.dump_to_json(subsampled_electron_lifetime, "electron_lifetime")

    cryostat_pressure = PsqlDB.get_cryostat_press_data(table_prefix=config_psql["cryopress_table_prefix"], tagid=config_psql["cryopress_tagid"])
    subsampled_cryostat_pressure = PsqlDB.get_subsampled_formatted_data(cryostat_pressure, "cryostat_pressure", subsample=subsample)
    PsqlDB.dump_to_json(subsampled_cryostat_pressure, "cryostat_pressure")

def get_gizmo_ground_tag(influxDB):
    database="gizmo"
    measurement="resistance"
    ground_impedance=config_influx["ground_impedance"]
    ground_impedance_err=config_influx["ground_impedance_err"]

    variables = influxDB.fetch_measurement_fields(database, measurement)
    result_data = influxDB.fetch_measurement_data(database, measurement, variables)
    subsampled_data = influxDB.get_subsampled_formatted_data(database, measurement, variables, result_data, subsample=config_influx["subsample_time"])

    tag = "good ground"
    bad_ground_values = []
    for entry in subsampled_data:
        if "resistance" in entry:
            if entry["resistance"] < ground_impedance-ground_impedance_err or entry["resistance"] > ground_impedance+ground_impedance_err:
                tag = "bad ground"
                bad_ground_values.append(entry)
        else:
            print("ERROR: No resistance field in data. This function can only be used to calculate good or bad grounding with gizmo")
            return
    if bad_ground_values: print(f"WARNING: Bad grounding detected at these times {bad_ground_values}")

    return tag

def calculate_effective_shell_resistances(influxDB):
    database="HVmonitoring"
    measurement="Raspi"
    variables = ["CH0", "CH1", "CH2", "CH3"]

    result_data = influxDB.fetch_measurement_data(database, measurement, variables)
    formatted_data = influxDB.get_subsampled_formatted_data(database, measurement, variables, result_data, subsample=None)

    effective_shell_resistances = []

    for var in variables:
        effective_shell_resistances.append(get_mean_measurement(formatted_data, var))

    return effective_shell_resistances

def calculate_electric_fields(influxDB):
    database="HVmonitoring"
    measurement="SPELLMAN_HV"
    variables = ["Voltage"]

    result_data = influxDB.fetch_measurement_data(database, measurement, variables)
    formatted_data = influxDB.get_subsampled_formatted_data(database, measurement, variables, result_data, subsample=None)

    mean_voltage = get_mean_measurement(formatted_data, variables[0])
    electric_fields =  []
    eff_resistance= calculate_effective_shell_resistances(influxDB=influxDB)
    pick_off_resistance=config_influx["pick_off_resistance"]
    drift_dist=config_influx["drift_dist"]

    for R in eff_resistance:
        E = mean_voltage*(1-(pick_off_resistance/(pick_off_resistance+R)))/drift_dist
        electric_fields.append(E)
        
    return electric_fields

def dump_SC_data(influxDB, PsqlDB, config_file, json_filename="", dump_all_data=False):

    load_config(config_file)

    if(dump_all_data):
        influx_blind_dump(influxDB=influxDB)
        psql_blind_dump(PsqlDB=PsqlDB)
    else:
        ground_tag = get_gizmo_ground_tag(influxDB=influxDB)
        electron_lifetime = PsqlDB.get_purity_monitor_data(tablename=config_psql["purity_mon_table"], last_value=True)
        electric_fields = calculate_electric_fields(influxDB=influxDB)
        data = {
        "Gizmo grounding": ground_tag,
        "Purity monitor": {
            "last timestamp": electron_lifetime[0].isoformat(),
            "electron lifetime (ms)": electron_lifetime[1]
            },
        "Electric fields (V/m)": {
            "Module 0": electric_fields[0],
            "Module 1": electric_fields[1],
            "Module 2": electric_fields[2],
            "Module 3": electric_fields[3],
            }
        }
        with open(json_filename, "w") as json_file:
            json.dump(data, json_file, indent=4)

        print(f"Slow controls data has been dumped into {json_filename}")
