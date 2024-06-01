import yaml
import datetime
import argparse
from dateutil import parser as date_parser
from DB import *
from DataManager import *

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
    """Parse the date string and add default times if only date is provided."""
    dt = date_parser.parse(date_str)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        if is_start:
            return datetime.datetime.combine(dt.date(), datetime.time.min)
        else:
            return datetime.datetime.combine(dt.date(), datetime.time.max)
    return dt

def main():
    parser = argparse.ArgumentParser(description="Query data from databases and save it as JSON.")
    parser.add_argument('--start', type=str, nargs='?', help="Start time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--end', type=str, nargs='?', help="End time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--measurement', type=str, nargs='?', help="Measurement name to query")
    parser.add_argument('--subsample', type=str, default=None, help="Subsample interval in s like '60S' (optional)")

    args = parser.parse_args()

    if not args.start or not args.end or not args.measurement:
        parser.print_help()
        print("\nError: start, end, and measurement are required arguments.")
        return

    start = datetime.datetime.now()

    try:
        run_start = parse_datetime(args.start, is_start=True)
        run_end = parse_datetime(args.end, is_start=False)
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return

    measurement = args.measurement
    subsample = args.subsample

    cred_config_file = "config/credentials.yaml"
    param_config_file = "config/parameters.yaml"

    print(f"----------------------------------------Fetching data for the time period {run_start} to {run_end}----------------------------------------")

    with open(cred_config_file, "r") as yaml_file:
        cred_config = yaml.safe_load(yaml_file)

    PsqlDB = PsqlDBManager(config=cred_config["psql"], run_start=run_start, run_end=run_end)
    influxDB = InfluxDBManager(config=cred_config["influxdb"], run_start=run_start, run_end=run_end)

    with open(param_config_file, "r") as yaml_file:
        param_config = yaml.safe_load(yaml_file)

    config_influx = param_config["influxdb"]
    config_psql = param_config["psql"]

    try:
        source, info = get_measurement_info(measurement, config_influx, config_psql)
    except ValueError as e:
        print(e)
        return

    if source == 'influx':
        database, measurement, variables = info
        dump(DataManager(influxDB.fetch_measurement_data(database=database, measurement=measurement, variables=variables)).format(source="influx", variables=variables, subsample_interval=subsample), filename=influxDB.make_filename(database, measurement))
    elif source == 'psql_cryostat':
        table_prefix, variable, tagid = info
        dump(DataManager(PsqlDB.get_cryostat_data(table_prefix=table_prefix, variable=variable, tagid=tagid)).format(source="psql", variables=[variable], subsample_interval=subsample), filename=PsqlDB.make_filename(variable))
    elif source == 'psql_purity_mon':
        tablename, measurement, variable = info
        dump(DataManager(PsqlDB.get_purity_monitor_data(tablename=tablename, variables=[variable])).format(source="psql", variables=[measurement], subsample_interval=subsample), filename=PsqlDB.make_filename(measurement))
    else:
        print(f"Measurement '{measurement}' not found in the configuration.")

    end = datetime.datetime.now()

    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
