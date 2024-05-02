from DB import *

def run_blob_maker(run_start, run_end, plot=False, config=None):


    psql_config_file = config['psql']
    psql = PsqlDB(psql_config_file, run_start, run_end)
    cryo_press = psql.fetch_data()
    psql.dump_to_json(cryo_press)


    if plot: psql.plot_cryo_pressure(cryo_press)



def main():

    start = datetime.datetime.now()

    run_start = '2024-04-10 18:09:59.804494'
    run_end = '2024-04-11 18:09:59.804494'
    plot = False

    config_filenames = {
        'influxdb': '',
        'psql': 'config/psql_config.yaml',
        'prometheus': ''
    }

    run_blob_maker(run_start, run_end, plot, config_filenames)

    end = datetime.datetime.now()
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
