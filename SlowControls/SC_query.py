import yaml
import datetime
import argparse
from dateutil import parser as date_parser
from DB import *
from SC_utils import *

def get_measurement_info(measurement, config_influx, config_psql):
    if measurement in config_influx.get('influx_SC_special_dict', {}):
        return 'influx', config_influx['influx_SC_special_dict'][measurement]
    elif measurement in config_psql.get('cryostat_tag_dict', {}):
        table_prefix = config_psql['cryo_table_prefix']
        variable = measurement
        tagid = config_psql['cryostat_tag_dict'][measurement]
        return 'psql_cryostat', (table_prefix, variable, tagid)
    elif measurement in config_psql.get('purity_mon_variables', {}):
        tablename = config_psql['purity_mon_table']
        variable = config_psql['purity_mon_variables'][measurement]
        return 'psql_purity_mon', (tablename, measurement, variable)
    raise ValueError(f"Configuration does not support fetching {measurement}. Check influx_SC_special_dict, cryostat_tag_dict, purity_mon_variables in config/parameters.yaml to make sure your measurement is present there")

def parse_datetime(date_str, is_start):
    dt = date_parser.parse(date_str)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        if is_start:
            return datetime.datetime.combine(dt.date(), datetime.time.min)
        else:
            return datetime.datetime.combine(dt.date(), datetime.time.max)
    return dt

def blob_maker(start, end, measurement, subsample=None, run_number=None):
    query_start = datetime.datetime.now()

    cred_config_file = "config/credentials.yaml"
    param_config_file = "config/parameters.yaml"

    print(f"----------------------------------------Fetching data for the time period {start} to {end}----------------------------------------")

    with open(cred_config_file, "r") as yaml_file:
        cred_config = yaml.safe_load(yaml_file)

    PsqlDB = PsqlDBManager(config=cred_config["psql"], start=start, end=end)
    influxDB = InfluxDBManager(config=cred_config["influxdb"], start=start, end=end)

    with open(param_config_file, "r") as yaml_file:
        param_config = yaml.safe_load(yaml_file)

    config_influx = param_config["influxdb"]
    config_psql = param_config["psql"]

    if measurement == "runsdb":
        if not run_number:
            raise ValueError("run_number is a required argument when measurement is runsdb.")
        output_json_filename = f"SlowControls_run-{run_number}_{start.isoformat()}_{end.isoformat()}.json"
        dump_SC_data(influxDB=influxDB, PsqlDB=PsqlDB, config_file=param_config_file, json_filename=output_json_filename, subsample=subsample, dump_all_data=False)

    elif measurement == "all":
        dump_SC_data(influxDB=influxDB, PsqlDB=PsqlDB, config_file=param_config_file, subsample=subsample, dump_all_data=True)

    else:
        source, info = get_measurement_info(measurement, config_influx, config_psql)

        if source == 'influx':
            database, measurement, variables = info
            dump_single_influx(influxDB=influxDB, database=database, measurement=measurement, variables=variables, subsample=subsample)
        elif source == 'psql_cryostat':
            table_prefix, variable, tagid = info
            dump_single_cryostat(PsqlDB=PsqlDB, table_prefix=table_prefix, variable=variable, tagid=tagid, subsample=subsample)
        elif source == 'psql_purity_mon':
            tablename, measurement, variable = info
            dump_single_prm(PsqlDB=PsqlDB, tablename=tablename, measurement=measurement, variable=variable, subsample=subsample)
        else:
            print(f"Measurement '{measurement}' not found in the configuration.")

    query_end = datetime.datetime.now()
    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

def main():
    parser = argparse.ArgumentParser(description="Query data from databases and save it as JSON.")
    parser.add_argument('--start', type=str, required=True, help="Start time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--end', type=str, required=True, help="End time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--measurement', type=str, required=True, help="Measurement name to query. Use 'runsdb' for runs database and 'all' if you want all the measurments in the parameters/config.yaml (influx_SC_data_dict, cryostat_tag_dict, purity_mon_variables)")
    parser.add_argument('--subsample', type=str, default=None, help="Subsample interval in s like '60S' (optional)")
    parser.add_argument('--run_number', type=str, default=None, help="Run number for runsdb (required when measurement is runsdb)")

    args = parser.parse_args()

    try:
        start = parse_datetime(args.start, is_start=True)
        end = parse_datetime(args.end, is_start=False)
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return

    blob_maker(start=start, end=end, measurement=args.measurement, subsample=args.subsample, run_number=args.run_number)

if __name__ == "__main__":
    main()
