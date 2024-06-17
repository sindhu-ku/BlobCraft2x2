#!/usr/bin/env python3

import os
from datetime import datetime, time
import argparse
from dateutil import parser as date_parser
from ..DB import InfluxDBManager, PsqlDBManager
from .SC_utils import *
from ..Beam.beam_query import get_beam_summary

class SCQueryGlobals:
    def __init__(self):
        measurement=''
        param_config_file = ''
        param_config=None
        influxDB=None
        psqlDB=None
        run=-1
        subrun=-1
        subsample=None
        start=None
        end=None
        config_influx=None
        config_pqsl=None
        output_dir=None

glob = SCQueryGlobals()

def get_measurement_info():
    if glob.measurement in glob.config_influx.get('influx_SC_special_dict', {}):
        return 'influx', glob.config_influx['influx_SC_special_dict'][glob.measurement]
    elif glob.measurement in glob.config_pqsl.get('cryostat_tag_dict', {}):
        table_prefix = glob.config_pqsl['cryo_table_prefix']
        variable = glob.measurement
        tagid = glob.config_pqsl['cryostat_tag_dict'][glob.measurement]
        return 'psql_cryostat', (table_prefix, variable, tagid)
    elif glob.measurement in glob.config_pqsl.get('purity_mon_variables', {}):
        tablename = glob.config_pqsl['purity_mon_table']
        variable = [glob.config_pqsl['purity_mon_variables'][glob.measurement]]
        return 'psql_purity_mon', (tablename, [glob.measurement], variable)
    elif glob.measurement == "purity_monitor":
        tablename = glob.config_pqsl['purity_mon_table']
        table = glob.config_pqsl['purity_mon_variables']
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

def process_single_instance():
    glob.config_influx = glob.param_config["influxdb"]
    glob.config_pqsl = glob.param_config["psql"]

    if glob.measurement == "runsdb":
        if glob.subrun:
            output_json_filename = os.path.join(glob.output_dir, f"SlowControls_run-{glob.run}_subrun-{glob.subrun}_{glob.start.isoformat()}_{glob.end.isoformat()}")
        else:
            output_json_filename =os.path.join(glob.output_dir, f"SlowControls_run-{glob.run}_{start.isoformat()}_{glob.end.isoformat()}")

        data = dump_SC_data(influxDB_manager=glob.influxDB, psqlDB_manager=glob.psqlDB, config_file=glob.param_config_file, json_filename=output_json_filename, subsample=glob.subsample, dump_all_data=False)
        dump(data, output_json_filename)

    elif glob.measurement == "all":
        dump_SC_data(influxDB_manager=glob.influxDB, psqlDB_manager=glob.psqlDB, config_file=glob.param_config_file, subsample=glob.subsample, dump_all_data=True, individual=True, output_dir=glob.output_dir)

    else:
        source, info = get_measurement_info()
        if source == 'influx':
            database, measurement, variables = info
            dump_single_influx(influxDB=glob.influxDB, database=database, measurement=measurement, variables=variables, subsample=glob.subsample, output_dir=glob.output_dir)
        elif source == 'psql_cryostat':
            table_prefix, variable, tagid = info
            dump_single_cryostat(psqlDB=glob.psqlDB, table_prefix=table_prefix, variable=variable, tagid=tagid, subsample=glob.subsample,  output_dir=glob.output_dir)
        elif source == 'psql_purity_mon':
            tablename, measurements, variables = info
            dump_single_prm(psqlDB=glob.psqlDB, tablename=tablename, measurements=measurements, variables=variables, subsample=glob.subsample,  output_dir=glob.output_dir)
        else:
            print(f"Measurement '{measurement}' not found in the configuration.")

def SC_blob_maker(measurement_name, start_time=None, end_time=None, subsample_interval=None, run_number=None, subrun_number=None, subrun_dict=None, output_directory=None):
    query_start = datetime.now()

    if (measurement_name=="runsdb" or measurement_name=="ucondb")  and not subrun_dict and not start_time and not end_time:
        raise ValueError("ERROR: please provide start and end times or a subrun_dict!")

    if (measurement_name=="runsdb" or measurement_name=="ucondb")  and not run_number:
        raise ValueError("ERROR: You must provide a run number!")

    glob.measurement=measurement_name
    glob.run=run_number
    glob.subrun=subrun_number
    glob.subsample=subsample_interval
    glob.output_dir=output_directory

    if output_directory:
        glob.output_dir=output_directory
        os.makedirs(glob.output_dir, exist_ok=True)
    else:
        glob.output_dir=os.getcwd()

    cred_config_file = "config/SC_credentials.yaml"
    glob.param_config_file = "config/SC_parameters.yaml"

    cred_config = load_config(cred_config_file)
    glob.param_config = load_config(glob.param_config_file)

    glob.psqlDB = PsqlDBManager(config=cred_config["psql"])
    glob.influxDB = InfluxDBManager(config=cred_config["influxdb"])

    if subrun_dict:
        if not (glob.measurement == "runsdb" or glob.measurement == "ucondb"): raise ValueError("Unrecognized measurement value. Only give 'ucondb' or 'runsdb' if subrun_dict is used!")

        data = {}
        output_json_filename=''
        for subrun, times in subrun_dict.items():
            if not start_time: start_str=times['start_time']
            end_str=times['end_time']

            start_t, end_t=times['start_time'], times['end_time']
            try:
                glob.start = parse_datetime(start_t, is_start=True)
                glob.end = parse_datetime(end_t, is_start=False)
            except ValueError as e:
                print(f"Error parsing date: {e}")
                return
            print(f"----------------------------------------Fetching Slow Controls data for the time period {glob.start} to {glob.end}, subrun={subrun}----------------------------------------")
            glob.psqlDB.set_time_range(glob.start, glob.end)
            glob.influxDB.set_time_range(glob.start, glob.end)

            if glob.measurement=="runsdb":
                beam_data = get_beam_summary(start_t, end_t)
                SC_data = dump_SC_data(influxDB_manager=glob.influxDB, psqlDB_manager=glob.psqlDB, config_file=glob.param_config_file, subsample=glob.subsample, dump_all_data=False)
                data[f'subrun_{subrun}'] = {**SC_data, **beam_data}
            if glob.measurement=="ucondb": data[f'subrun_{subrun}'] = dump_SC_data(influxDB_manager=glob.influxDB, psqlDB_manager=glob.psqlDB, config_file=glob.param_config_file, subsample=glob.subsample, dump_all_data=True)


        if glob.measurement=="runsdb":
                query_end = datetime.now()
                print("\n")
                print("----------------------------------------END OF SC QUERYING AND BLOB MAKING----------------------------------------")
                print("Total querying and blob making time in s: ", query_end - query_start)
                return data
        if glob.measurement=="ucondb": dump(data, os.path.join(glob.output_dir, f"SlowControls_all_ucondb_measurements_run-{glob.run}_{start_str}_{end_str}"))

    else:
        print(f"----------------------------------------Fetching Slow Controls data for the time period {start_time} to {end_time}----------------------------------------")
        try:
            glob.start = parse_datetime(start_time, is_start=True)
            glob.end = parse_datetime(end_time, is_start=False)
        except ValueError as e:
            print(f"Error parsing date: {e}")
            return
        glob.psqlDB.set_time_range(glob.start, glob.end)
        glob.influxDB.set_time_range(glob.start, glob.end)
        process_single_instance()

    glob.influxDB.close_connection()
    glob.psqlDB.close_connection()

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
    parser.add_argument('--output_dir', type=str, default=None, help="Directory to save the output files")

    args = parser.parse_args()

    SC_blob_maker(start_time=args.start, end_time=args.end, measurement_name=args.measurement, subsample_interval=args.subsample, run_number=args.run, output_directory=args.output_dir)

if __name__ == "__main__":
    main()
