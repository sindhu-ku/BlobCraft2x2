import yaml
import datetime
from DB import *
from SC_utils import dump_SC_data

def main():

    start = datetime.datetime.now()

    run_start = '2024-05-23 18:09:59.804494'
    run_end = '2024-05-24 18:09:59.804494'

    cred_config_file = 'config/credentials.yaml'
    param_config_file = 'config/parameters.yaml'

    run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
    run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
    output_json_filename = f'SlowControls_{run_start.isoformat()}_{run_end.isoformat()}.json'

    print(f"----------------------------------------Fetching data for the time period {run_start} to {run_end}----------------------------------------")

    cred_config = None
    with open(cred_config_file, 'r') as yaml_file:
        cred_config=yaml.safe_load(yaml_file)

    PsqlDB = PsqlDBManager(config=cred_config['psql'],  run_start=run_start, run_end=run_end)

    influxDB = InfluxDBManager(config=cred_config['influxdb'], run_start=run_start, run_end=run_end)

    dump_SC_data(influxDB=influxDB, PsqlDB=PsqlDB, config_file=param_config_file, json_filename=output_json_filename, dump_all_data=False)

    end = datetime.datetime.now()

    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
