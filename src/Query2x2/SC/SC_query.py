#!/usr/bin/env python3

from datetime import datetime, time
import argparse
from dateutil import parser as date_parser
from ..DB import InfluxDBManager, PsqlDBManager
from .SC_utils import *


measurement=''
param_config_file = ''
param_config=None
influxDB=None
PsqlDB=None
run=-1
subrun=-1
subsample=None
start=None
end=None
config_influx=None
config_psql=None

def get_measurement_info():
    if measurement in config_influx.get('influx_SC_special_dict', {}):
        return 'influx', config_influx['influx_SC_special_dict'][measurement]
    elif measurement in config_psql.get('cryostat_tag_dict', {}):
        table_prefix = config_psql['cryo_table_prefix']
        variable = measurement
        tagid = config_psql['cryostat_tag_dict'][measurement]
        return 'psql_cryostat', (table_prefix, variable, tagid)
    elif measurement in config_psql.get('purity_mon_variables', {}):
        tablename = config_psql['purity_mon_table']
        variable = [config_psql['purity_mon_variables'][measurement]]
        return 'psql_purity_mon', (tablename, measurement, variable)
    elif measurement == "purity_monitor":
        tablename = config_psql['purity_mon_table']
        table = config_psql['purity_mon_variables']
        measurements = list(table.keys())
        variables = list(table.values())
        return 'psql_purity_mon', (tablename, measurements, variables)
    raise ValueError(f"Configuration does not support fetching {measurement}. Check influx_SC_special_dict, cryostat_tag_dict, purity_mon_variables in config/parameters.yaml to make sure your measurement is present there")

def parse_datetime(date_str, is_start):
    dt = date_parser.parse(date_str)
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=chicago_tz)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        if is_start:
            return datetime.combine(dt.date(), time.min, tzinfo=chicago_tz)
        else:
            return datetime.combine(dt.date(), time.max, tzinfo=chicago_tz)
    return dt.astimezone(chicago_tz)

def process_single_instance(measurement):
    global config_influx, config_psql

    config_influx = param_config["influxdb"]
    config_psql = param_config["psql"]

    if measurement == "runsdb":
        if subrun:
            output_json_filename = f"SlowControls_run-{run}_subrun-{subrun}_{start.isoformat()}_{end.isoformat()}.json"
        else:
            output_json_filename = f"SlowControls_run-{run}_{start.isoformat()}_{end.isoformat()}.json"
        data = dump_SC_data(influxDB_manager=influxDB, PsqlDB_manager=PsqlDB, config_file=param_config_file, json_filename=output_json_filename, subsample=subsample, dump_all_data=False)
        dump(data, output_json_filename)

    elif measurement == "all":
        output_json_filename = f"SlowControls_{start.isoformat()}_{end.isoformat()}.json"
        data = dump_SC_data(influxDB_manager=influxDB, PsqlDB_manager=PsqlDB, config_file=param_config_file, subsample=subsample, dump_all_data=True)
        dump(data, output_json_filename)

    else:
        source, info = get_measurement_info()
        if source == 'influx':
            database, measurement, variables = info
            dump_single_influx(influxDB=influxDB, database=database, measurement=measurement, variables=variables, subsample=subsample)
        elif source == 'psql_cryostat':
            table_prefix, variable, tagid = info
            dump_single_cryostat(PsqlDB=PsqlDB, table_prefix=table_prefix, variable=variable, tagid=tagid, subsample=subsample)
        elif source == 'psql_purity_mon':
            tablename, measurements, variables = info
            dump_single_prm(PsqlDB=PsqlDB, tablename=tablename, measurements=measurements, variables=variables, subsample=subsample)
        else:
            print(f"Measurement '{measurement}' not found in the configuration.")

def SC_blob_maker(measurement_name, start_time=None, end_time=None, subsample_interval=None, run_number=None, subrun_number=None, subrun_dict=None):
    query_start = datetime.now()

    global measurement, param_config_file, param_config, influxDB, PsqlDB, run, subrun, subsample, start, end

    measurement=measurement_name
    run=run_number
    subrun=subrun_number
    subsample=subsample_interval

    if not measurement=="runsdb" and not subrun_dict and not start_time and not end_time:
        raise ValueError("ERROR: please provide start and end times or a subrun_dict!")

    if measurement=="runsdb" and not run:
        raise ValueError("ERROR: You must provide a run number!")

    cred_config_file = "config/SC_credentials.yaml"
    param_config_file = "config/SC_parameters.yaml"

    cred_config = load_config(cred_config_file)
    param_config = load_config(param_config_file)

    PsqlDB = PsqlDBManager(config=cred_config["psql"])
    influxDB = InfluxDBManager(config=cred_config["influxdb"])
    if subrun_dict:
        start_times = []
        end_times = []
        subruns = []

        for subrun, times in subrun_dict.items():
            start_times.append(times['start_time'])
            end_times.append(times['end_time'])
            subruns.append(subrun)

        if len(subruns) != len(start_times) != len(end_times):
            raise ValueError("ERROR: lengths of start, end times and subruns are not equal!")

        data = {}

        if measurement=="runsdb": output_json_filename = f"SlowControls_summary_run-{run}_{start_times[0].isoformat()}_{end_times[-1].isoformat()}.json"
        elif measurement=="all":  output_json_filename = f"SlowControls_all-measurements_run-{run}_{start_times[0].isoformat()}_{end_times[-1].isoformat()}.json"
        else: raise ValueError("Unrecognized measurement value. Only give 'all' or 'runsb' if subrun_dict is used!")

        for  subrun, start, end in zip(subruns, start_times, end_times):
            print(f"----------------------------------------Fetching Slow Controls data for the time period {start} to {end}, subrun={subrun}----------------------------------------")
            PsqlDB.set_time_range(start, end)
            influxDB.set_time_range(start, end)
            if measurement=="runsdb": data[f'subrun_{subrun}'] = dump_SC_data(influxDB_manager=influxDB, PsqlDB_manager=PsqlDB, config_file=param_config_file, subsample=subsample, dump_all_data=False)
            elif measurement=="all": data[f'subrun_{subrun}'] = dump_SC_data(influxDB_manager=influxDB, PsqlDB_manager=PsqlDB, config_file=param_config_file, subsample=subsample, dump_all_data=True)
            else: raise ValueError("Unrecognized measurement value. Only give 'all' or 'runsb' if subrun_dict is used!")

        dump(data, output_json_filename)
    else:
        print(f"----------------------------------------Fetching Slow Controls data for the time period {start_time} to {end_time}----------------------------------------")
        try:
            start = parse_datetime(start_time, is_start=True)
            end = parse_datetime(end_time, is_start=False)
        except ValueError as e:
            print(f"Error parsing date: {e}")
            return
        PsqlDB.set_time_range(start, end)
        influxDB.set_time_range(start, end)
        process_single_instance(measurement)

    influxDB.close_connection()
    PsqlDB.close_connection()

    query_end = datetime.now()
    print("\n")
    print("----------------------------------------END OF SC QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

def main():
    parser = argparse.ArgumentParser(description="Query data from databases and save it as JSON.")
    parser.add_argument('--start', type=str, help="Start times for the query (comma-separated if multiple, various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--end', type=str, help="End times for the query (comma-separated if multiple, various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--measurement', type=str, required=True, help="Measurement name to query. Use 'runsdb' for runs database and 'all' if you want all the measurements in the parameters/config.yaml (influx_SC_data_dict, cryostat_tag_dict, purity_mon_variables)")
    parser.add_argument('--run', type=int, default=None, help="Run number for runsdb (required when measurement is runsdb)")
    parser.add_argument('--subsample', type=str, default=None, help="Subsample interval in s like '60S' (optional)")

    args = parser.parse_args()

    SC_blob_maker(start_time=args.start, end_time=args.end, measurement_name=args.measurement, subsample_interval=args.subsample, run_number=args.run)

if __name__ == "__main__":
    main()
