#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo
from ..DataManager import *
from .. import local_tz, SC_config

class SCUtilsGlobals:
    def __init__(self):
        config_influx = {}
        config_psql = {}
        subsample_interval = None
        influxDB = None
        psqlDB = None
        individual = False
        output_dir = None


glob = SCUtilsGlobals()

def get_mean(data, varname):
    df = pd.DataFrame(data)
    return df[varname].mean()

def influx_blind_dump():
    databases = glob.config_influx.get("influx_SC_db", [])
    merged_data = {}
    for database in databases:
        measurements = glob.influxDB.fetch_measurements(database)
        for measurement in measurements:
            variables = glob.influxDB.fetch_measurement_fields(database, measurement)
            result_data = DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables))
            formatted_data = result_data.format(source="influx", variables=variables, subsample_interval=glob.subsample_interval)
            if not glob.individual: merged_data[f'{database}_{measurement}'] = formatted_data
            else:
                out_filename = os.path.join(glob.output_dir, glob.influxDB.make_filename(database, measurement))
                dump(formatted_data, filename=out_filename)

    if not glob.individual: return merged_data

def psql_blind_dump():
    merged_data = {}
    purity_mon_dict = glob.config_psql.get("purity_mon_variables", {})
    varnames = list(purity_mon_dict.keys())
    variables = list(purity_mon_dict.values())
    purity_monitor = DataManager(glob.psqlDB.get_purity_monitor_data(tablename=glob.config_psql["purity_mon_table"], variables=variables))
    formatted_data_prm = purity_monitor.format(source="psql", variables=varnames, subsample_interval=glob.subsample_interval)
    if not glob.individual:
        merged_data["purity_monitor"] = formatted_data_prm
        return merged_data
    else:
        out_filename = os.path.join(glob.output_dir,  glob.psqlDB.make_filename("purity_monitor"))
        dump(formatted_data_prm, filename=out_filename)

def get_influx_db_meas_vars(meas_name):
    database, measurement, variables = glob.config_influx.get("influx_SC_special_dict", {}).get(meas_name, [None, None, None])
    if not (database and measurement and variables):
        raise ValueError(f"The given measurement name {meas_name} is not in the dict. Check influx_SC_special_dict in config/parameters.yaml")
    return database, measurement, variables

def get_tag(measurement_name, field_name, threshold):
    database, measurement, variables = get_influx_db_meas_vars(measurement_name)

    data = DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=glob.subsample_interval)

    tag = "bad"
    bad_values = []

    if not data:
        return "no data", 0.0

    for entry in data:
        if entry[field_name] < threshold:
            bad_values.append(entry)

    bad_percent = len(bad_values) * 100. / len(data)
    if bad_values:
        print(f"WARNING: {bad_percent}% of bad values detected at these times: {bad_values}")
    else:
        tag = "good"

    return tag, bad_percent
    #With psql:
    # data = DataManager(glob.psqlDB.get_cryostat_data(table_prefix=glob.config_psql["cryo_table_prefix"], variable="LAr_level", tagid=glob.config_psql["cryostat_tag_dict"]["LAr_level"])).format(source="psql", variables=["LAr_level"], subsample_interval=glob.subsample_interval)
def get_last_O2():
    database, measurement, variables = get_influx_db_meas_vars("O2_ppb")
    data = DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=glob.subsample_interval)
    if data: return data[-1]
    else: return {'time': datetime.now(), 'magnitude': '0.0'}

def get_spellman_hv():
    database, measurement, variables = get_influx_db_meas_vars("set_voltage")
    data = DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=glob.subsample_interval)
    if not data: return 0.0
    mean = get_mean(data, variables[0])
    return mean

def get_mod_voltages():
    mod_voltages = np.zeros(4)
    database, measurement, variables = get_influx_db_meas_vars("pick_off_voltages")
    data = DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=glob.subsample_interval)
    if not data:
        return mod_voltages
    for i, var in enumerate(variables):
        mod_voltages[i] = get_mean(data, var)
    return mod_voltages

def dump_SC_data(influxDB_manager, psqlDB_manager, subsample=None, json_filename="", dump_all_data=False, individual=False, output_dir=None):
    config = SC_config
    glob.config_influx = config["influxdb"]
    glob.config_psql = config["psql"]
    glob.subsample_interval = subsample
    glob.influxDB = influxDB_manager
    glob.psqlDB = psqlDB_manager
    glob.individual = individual
    if output_dir: glob.output_dir = output_dir
    else: glob.output_dir=os.getcwd()
    if dump_all_data:
        influx_data = influx_blind_dump()
        psql_data = psql_blind_dump()
        if not glob.individual:
            merged_data = {**influx_data, **psql_data}
            return merged_data
    else:
        ground_tag, bad_ground_per = get_tag("ground_impedance", "resistance", glob.config_influx["good_ground_impedance"])
        LAr_tag, bad_LAr_per = get_tag("LAr_level_mm", "magnitude", glob.config_influx["good_LAr_level"])
        O2_meas = get_last_O2()
        electron_lifetime = glob.psqlDB.get_purity_monitor_data(tablename=glob.config_psql["purity_mon_table"], variables=["prm_lifetime"], last_value=True)
        set_voltage = get_spellman_hv()
        mod_voltages = get_mod_voltages()
        electric_fields = mod_voltages/(1e3*glob.config_influx["drift_dist"])

        data = {
            "Gizmo_grounding": {
                "quality": ground_tag,
                "bad_values_percent": bad_ground_per
            },
            "Liquid_Argon_level": {
                "quality": LAr_tag,
                "bad_values_percent": bad_LAr_per
            },
            "Purity_monitor": {
                "last_timestamp": pd.to_datetime(electron_lifetime[0], utc=True).astimezone(local_tz).isoformat(),
                "electron_lifetime_ms": electron_lifetime[1]*1e3
            },
            "O2_ppb": {
                "last_timestamp": O2_meas['time'],
                "value": O2_meas['magnitude']
            },
            "Mean_Spellman_set_voltage_kV": set_voltage,
            "Mean_module_voltages_kV": {
                "Module0": mod_voltages[0],
                "Module1": mod_voltages[1],
                "Module2": mod_voltages[2],
                "Module3": mod_voltages[3]
            },
            "Electric_fields_V_per_cm": {
                "Module0": electric_fields[0],
                "Module1": electric_fields[1],
                "Module2": electric_fields[2],
                "Module3": electric_fields[3]
            }
        }

        return data

def dump_single_influx(influxDB, database, measurement, variables=[], subsample=None, output_dir=None):
    if not variables: variables = influxDB.fetch_measurement_fields(database, measurement)
    out_filename = os.path.join(output_dir, influxDB.make_filename(database, measurement))
    dump(DataManager(influxDB.fetch_measurement_data(database=database, measurement=measurement, variables=variables)).format(source="influx", variables=variables, subsample_interval=subsample), filename=out_filename)

def dump_single_cryostat(psqlDB, table_prefix, variable, tagid, subsample=None, output_dir=None):
    out_filename = os.path.join(output_dir,  psqlDB.make_filename(variable))
    dump(DataManager(psqlDB.get_cryostat_data(table_prefix=table_prefix, variable=variable, tagid=tagid)).format(source="psql", variables=[variable], subsample_interval=subsample), filename=out_filename)

def dump_single_prm(psqlDB, tablename, measurements, variables, subsample=None, output_dir=None):
    out_filename = os.path.join(output_dir,  psqlDB.make_filename('_'.join(measurements)))
    dump(DataManager(psqlDB.get_purity_monitor_data(tablename=tablename, variables=variables)).format(source="psql", variables=measurements, subsample_interval=subsample), filename=out_filename)
