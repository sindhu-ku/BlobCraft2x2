from DB import *
import yaml

def main():

    start = datetime.datetime.now()

    run_start = '2024-05-01 18:09:59.804494'
    run_end = '2024-05-02 18:09:59.804494'

    config_file = 'config/DB_config.yaml'

    print(f"----------------------------------------Fetching data for the time period {run_start} to {run_end}----------------------------------------")

    config = None
    with open(config_file, 'r') as yaml_file:
        config=yaml.safe_load(yaml_file)

    PsqlDB(config=config['psql'], run_start=run_start, run_end=run_end, subsample="1S").fetch_data()
    InfluxDBManager(config=config['influxdb'], run_start=run_start, run_end=run_end, subsample="1S").fetch_data()

    end = datetime.datetime.now()

    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
