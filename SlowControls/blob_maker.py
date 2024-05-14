from DB import *
import yaml

def run_blob_maker(run_start=0., run_end=0., config_file='', influx_data_dict=None):

    print(f"----------------------------------------Fetching data for the time period {run_start} to {run_end}----------------------------------------")

    config = None
    with open(config_file, 'r') as yaml_file:
        config=yaml.safe_load(yaml_file)

    PsqlDB(config=config['psql'], run_start=run_start, run_end=run_end).fetch_data()
    InfluxDBManager(config=config['influxdb'], data_dict=influx_data_dict, run_start=run_start, run_end=run_end).fetch_data()



def main():

    start = datetime.datetime.now()

    run_start = '2024-05-01 18:09:59.804494'
    run_end = '2024-05-02 18:09:59.804494'

    config_filename = 'config/DB_config.yaml'

    influx_data_dict = {
       'gizmo': [['resistance'], [['resistance']]],
       'module0_mpod0': [['PACMAN&FANS', 'VGAs', 'RTDs'], [['voltage', 'current', 'channel_temperature', 'channel_name']]],
       'module1_mpod0': [['PACMAN&FANS', 'VGAs', 'RTDs'], [['voltage', 'current', 'channel_temperature', 'channel_name']]],
       'module2_mpod1': [['PACMAN&FANS', 'VGAs', 'RTDs'], [['voltage', 'current', 'channel_temperature', 'channel_name']]],
       'module3_mpod1': [['PACMAN&FANS', 'VGAs', 'RTDs'], [['voltage', 'current', 'channel_temperature', 'channel_name']]],
       'HVmonitoring': [['SPELLMAN_HV', 'Raspi'], [['Voltage', 'Current'], ['Temperature']]],
       'VME_crate01': [['temperature', 'electrical_params'], [['channel_name','temperature'],['channel_name', 'voltage_sens', 'voltage_terminal', 'current']]],
       'VME_crate23': [['temperature', 'electrical_params'], [['channel_name','temperature'],['channel_name', 'voltage_sens', 'voltage_terminal', 'current']]],
       'ADC_crate': [['temperature', 'electrical_params'], [['channel_name','temperature'], ['channel_name', 'voltage_sens', 'voltage_terminal', 'current']]],
       'lrs_monitor': [['sipm_bias'], [['chan_nr', 'pos', 'value']]],
       'pt100' : [['temp'],[['pos', 'sens', 'value']]]
    }

    run_blob_maker(run_start, run_end, config_filename, influx_data_dict)

    end = datetime.datetime.now()
    print("\n")
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time: ", end-start)

if __name__ == "__main__":
    main()
