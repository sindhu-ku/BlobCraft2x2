#!/usr/bin/env python3

import os
import re
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo
from ..DataManager import *
from .. import local_tz, Global_config, SC_config

class SCUtilsGlobals:
    def __init__(self):
        self.config_influx = {}
        self.config_psql = {}
        self.subsample_interval = None
        self.influxDB = None
        self.psqlDB = None
        self.individual = False
        self.output_dir = None


glob = SCUtilsGlobals()

def get_mean(data, varname):
    df = pd.DataFrame(data)
    return df[varname].mean()

def influx_blind_dump():
    databases = glob.config_influx.get("influx_SC_db", [])
    merged_data = {}
    cryostat_meas = {tuple(glob.config_influx["influx_SC_special_dict"][name][:2]): name for name in glob.config_psql.get("cryostat_tag_dict", {})}
    for database in databases:
        measurements = glob.influxDB.fetch_measurements(database)
        for measurement in measurements:
            variables = glob.influxDB.fetch_measurement_fields(database, measurement)
            if "mpod_influx_to_dcs_switch" in glob.config_psql and re.match(r"module\d+_mpod\d+$", database):
                formatted_data = get_mpod_data(database, measurement, variables)
            elif [database, measurement] == glob.config_influx["influx_SC_special_dict"]["set_voltage"][:2]:
                formatted_data = get_spellman_data(glob.influxDB, glob.psqlDB, glob.subsample_interval, variables)
            elif (database, measurement) in cryostat_meas:
                formatted_data = get_cryostat_data(glob.influxDB, glob.psqlDB, cryostat_meas[(database, measurement)], glob.subsample_interval, variables)
            else:
                result_data = DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables))
                formatted_data = result_data.format(source="influx", variables=variables, subsample_interval=glob.subsample_interval)
            if not glob.individual: merged_data[f'{database}_{measurement}'] = formatted_data
            else:
                out_filename = os.path.join(glob.output_dir, glob.influxDB.make_filename(database, measurement))
                dump(formatted_data, filename=out_filename)

    if not glob.individual: return merged_data

MPOD_DCS_GROUPS = {"VGAs": "VGA_Card", "RTDs": "RTD", "PACMAN&FANS": "PACMAN|pacFAN|[Ii]nterlock"} # Influx measurement: dcs_prd channel names

def get_mpod_data(database, measurement, variables):
    # MPOD readings moved from InfluxDB to the PostgreSQL DCS archiver; read each part of the time range from where it was logged
    switch = parse_datetime(glob.config_psql["mpod_influx_to_dcs_switch"], is_start=True)
    start, end = glob.influxDB.start, glob.influxDB.end
    data = []
    if start < switch:
        glob.influxDB.set_time_range(start, min(end, switch))
        data += DataManager(glob.influxDB.fetch_measurement_data(database, measurement, variables)).format(source="influx", variables=variables, subsample_interval=glob.subsample_interval) or []
        glob.influxDB.set_time_range(start, end)

    module = re.match(r"module(\d+)_", database)
    if end > switch and measurement in MPOD_DCS_GROUPS:
        glob.psqlDB.set_time_range(max(start, switch), end)
        rows = glob.psqlDB.get_dcs_channels(f"^Mod{module.group(1)}-[^/]*({MPOD_DCS_GROUPS[measurement]})[^/]*/")
        glob.psqlDB.set_time_range(start, end)

        fields = glob.config_psql["mpod_dcs_variables"]
        df = pd.DataFrame(rows, columns=["name", "time", "value"])
        df[["channel_name", "variable"]] = df["name"].str.split("/", n=1, expand=True)
        df = df[df["variable"].isin(fields)]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["time"] = pd.to_datetime(df["time"]).dt.tz_convert(local_tz)
        for channel_name, df_channel in df.groupby("channel_name"):
            # values are stored only when they change, so hold each value until the next one
            df_channel = df_channel.pivot_table(index="time", columns="variable", values="value", aggfunc="last").rename(columns=fields)
            df_channel = df_channel.reindex(columns=list(fields.values()))
            if df_channel.isna().all().all(): continue # channel without numeric values
            df_channel.loc[end.astimezone(local_tz)] = None # hold the last value until the end of the time range
            df_channel = df_channel.sort_index().ffill()
            if glob.subsample_interval is not None:
                df_channel = df_channel.resample(glob.subsample_interval).mean().ffill()
            df_channel = df_channel.astype(object).where(df_channel.notna(), None)
            for time, row in df_channel.iterrows():
                data.append({"time": time.isoformat(), **row.to_dict(), "tags": {"channel_name": channel_name}})
    return data

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
    if measurement_name in glob.config_psql.get("cryostat_tag_dict", {}):
        data = get_cryostat_data(glob.influxDB, glob.psqlDB, measurement_name, glob.subsample_interval)
    else:
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

def get_last_O2():
    data = get_cryostat_data(glob.influxDB, glob.psqlDB, "O2_ppb", glob.subsample_interval)
    if data: return data[-1]
    else: return {'time': datetime.now(), 'magnitude': '0.0'}

def get_spellman_data(influxDB, psqlDB, subsample=None, influx_variables=None):
    # Spellman HV moved from InfluxDB to the PostgreSQL DCS archiver; read each part of the time range from where it was logged
    database, measurement, variables = SC_config["influxdb"]["influx_SC_special_dict"]["set_voltage"]
    config_psql = SC_config["psql"]
    switch = parse_datetime(config_psql["spellman_influx_to_dcs_switch"], is_start=True)
    start, end = influxDB.start, influxDB.end
    data = []
    if start < switch:
        influxDB.set_time_range(start, min(end, switch))
        data += DataManager(influxDB.fetch_measurement_data(database, measurement, influx_variables or variables)).format(source="influx", variables=influx_variables or variables, subsample_interval=subsample) or []
        influxDB.set_time_range(start, end)
    if end > switch:
        psqlDB.set_time_range(max(start, switch), end)
        data += DataManager(psqlDB.get_dcs_data(config_psql["spellman_dcs_channel"])).format(source="psql", variables=variables, subsample_interval=subsample) or []
        psqlDB.set_time_range(start, end)
    return data

def get_cryostat_data(influxDB, psqlDB, meas_name, subsample=None, influx_variables=None):
    # cryostat readings moved from InfluxDB to the PostgreSQL tag historian; read each part of the time range from where it was logged
    database, measurement, variables = SC_config["influxdb"]["influx_SC_special_dict"][meas_name]
    config_psql = SC_config["psql"]
    switch = parse_datetime(config_psql["cryostat_influx_to_psql_switch"], is_start=True)
    start, end = influxDB.start, influxDB.end
    data = []
    if start < switch:
        influxDB.set_time_range(start, min(end, switch))
        data += DataManager(influxDB.fetch_measurement_data(database, measurement, influx_variables or variables)).format(source="influx", variables=influx_variables or variables, subsample_interval=subsample) or []
        influxDB.set_time_range(start, end)
    if end > switch:
        psqlDB.set_time_range(max(start, switch), end)
        data += DataManager(psqlDB.get_cryostat_data(config_psql["cryo_table_prefix"], meas_name, config_psql["cryostat_tag_dict"][meas_name])).format(source="psql", variables=variables, subsample_interval=subsample) or []
        psqlDB.set_time_range(start, end)
    return data

def get_spellman_hv():
    data = get_spellman_data(glob.influxDB, glob.psqlDB, glob.subsample_interval)
    if not data: return 0.0
    mean = get_mean(data, SC_config["influxdb"]["influx_SC_special_dict"]["set_voltage"][2][0])
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

def get_data_2x2():
    ground_tag, bad_ground_per = get_tag("ground_impedance", "resistance", glob.config_influx["good_ground_impedance"])
    LAr_tag, bad_LAr_per = get_tag("LAr_level_mm", "magnitude", glob.config_influx["good_LAr_level"])
    O2_meas = get_last_O2()
    electron_lifetime = glob.psqlDB.get_purity_monitor_data(tablename=glob.config_psql["purity_mon_table"], variables=["prm_lifetime"], last_value=True)
    set_voltage = get_spellman_hv()
    mod_voltages = get_mod_voltages()
    electric_fields = mod_voltages*1e3/glob.config_influx["drift_dist"] # kV -> V/cm

    return {
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

def get_data_FSD():
    try:
        meas = glob.influxDB.fetch_measurement_data(
            'fsd_sc', 'PFD-5x5', ['mean("ch-1")'])
        hv = next(meas.get_points())['mean']
        return {'HV_kV': hv}
    except:
        return {'HV_kV': -1.}

def dump_SC_data(json_filename="", dump_all_data=False, individual=False, output_dir=None):
    config = SC_config
    from .SC_query import glob as glob_SC_query
    if "influxdb" in config:
        glob.config_influx = config["influxdb"]
        glob.influxDB = glob_SC_query.influxDB
    if "psql" in config:
        glob.config_psql = config["psql"]
        glob.psqlDB = glob_SC_query.psqlDB
    glob.subsample_interval = glob_SC_query.subsample
    glob.individual = individual
    if output_dir: glob.output_dir = output_dir
    else: glob.output_dir=os.getcwd()
    if dump_all_data:
        influx_data = influx_blind_dump()
        psql_data = psql_blind_dump()
        if not glob.individual:
            merged_data = {**influx_data, **psql_data}
            return merged_data
    elif Global_config['experiment'] == '2x2':
        return get_data_2x2()
    elif Global_config['experiment'] == 'FSD':
        return get_data_FSD()
    else:
        raise RuntimeError(f'Unknown experiment: {Global_config["experiment"]}')

def dump_single_influx(influxDB, database, measurement, variables=[], subsample=None, filename=None):
    if not variables: variables = influxDB.fetch_measurement_fields(database, measurement)
    dump(DataManager(influxDB.fetch_measurement_data(database=database, measurement=measurement, variables=variables)).format(source="influx", variables=variables, subsample_interval=subsample), filename=filename)

def dump_single_cryostat(psqlDB, table_prefix, variable, tagid, subsample=None, output_dir=None):
    out_filename = os.path.join(output_dir,  psqlDB.make_filename(variable))
    dump(DataManager(psqlDB.get_cryostat_data(table_prefix=table_prefix, variable=variable, tagid=tagid)).format(source="psql", variables=[variable], subsample_interval=subsample), filename=out_filename)

def dump_single_prm(psqlDB, tablename, measurements, variables, subsample=None, output_dir=None):
    out_filename = os.path.join(output_dir,  psqlDB.make_filename('_'.join(measurements)))
    dump(DataManager(psqlDB.get_purity_monitor_data(tablename=tablename, variables=variables)).format(source="psql", variables=measurements, subsample_interval=subsample), filename=out_filename)
