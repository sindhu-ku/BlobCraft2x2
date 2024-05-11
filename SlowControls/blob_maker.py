from DB import *

def run_blob_maker(run_start, run_end, plot=False, config=None):

    print(f"----------------------------------------Fetching data for the time period {run_start} to {run_end}----------------------------------------")

    psql_config_file = config['psql']
    psql = PsqlDB(psql_config_file, run_start, run_end)
    cryo_press = psql.fetch_data()
    psql.dump_to_json(cryo_press)

    influx_config_file = config['influxdb']
    influx_manager = InfluxDBManager(influx_config_file, "gizmo", "resistance", run_start, run_end)
    gizmo_res = influx_manager.fetch_data()
    influx_manager.dump_to_json(gizmo_res)

    if plot:
        print("\n")
        print(f"----------------------------------------Making validation plots----------------------------------------")
        psql.plot_cryo_pressure(cryo_press)
        influx_manager.plot_data(gizmo_res)


def main():

    start = datetime.datetime.now()

    run_start = '2024-04-10 18:09:59.804494'
    run_end = '2024-04-11 18:09:59.804494'
    plot = False

    config_filenames = {
        'influxdb': 'config/influxdb_config.yaml',
        'psql': 'config/psql_config.yaml',
        'prometheus': ''
    }

    run_blob_maker(run_start, run_end, plot, config_filenames)

    end = datetime.datetime.now()
    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
