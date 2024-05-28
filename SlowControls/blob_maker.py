from DB import *
import yaml
from SC_utils import dump_SC_data

def main():

    start = datetime.datetime.now()

    run_start = '2024-05-22 18:09:59.804494'
    run_end = '2024-05-23 18:09:59.804494'

    config_file = 'config/DB_config.yaml'

    print(f"----------------------------------------Fetching data for the time period {run_start} to {run_end}----------------------------------------")

    config = None
    with open(config_file, 'r') as yaml_file:
        config=yaml.safe_load(yaml_file)

    PsqlDB(config=config['psql'], run_start=run_start, run_end=run_end, subsample="60S").fetch_data(variable="cryostat_pressure")
    PsqlDB(config=config['psql'], run_start=run_start, run_end=run_end, subsample="60S").fetch_data(variable="electron_lifetime")
    influxDB = InfluxDBManager(config=config['influxdb'], run_start=run_start, run_end=run_end, subsample="60S")

    dump_SC_data(influxDB=influxDB, config=config['influxdb'], dump_all_data=False)

    end = datetime.datetime.now()

    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
