import yaml
import json

def influx_blind_dump(influxDB, influx_data_dict, subsample=None):
    for database, (measurements, variables) in influx_data_dict.items():
        if not variables:
            variables = [influxDB.fetch_measurement_fields(database, measurement) for measurement in measurements]
        for measurement, measurement_variables in zip(measurements, variables):
            result_data = influxDB.fetch_measurement_data(database, measurement, measurement_variables)
            formatted_data = influxDB.get_subsampled_formatted_data(database, measurement, measurement_variables, result_data, subsample=subsample)
            influxDB.dump_to_json(formatted_data, database, measurement)

def psql_blind_dump(PsqlDB, prm_tablename, prm_variablename, cryopress_table_prefix, cryopress_tagid, cryopress_variablename, subsample=None):
    electron_lifetime = PsqlDB.get_purity_monitor_data(tablename=prm_tablename)
    subsampled_electron_lifetime = PsqlDB.get_subsampled_formatted_data(electron_lifetime, prm_variablename, subsample=subsample)
    PsqlDB.dump_to_json(subsampled_electron_lifetime, prm_variablename)

    cryostat_pressure = PsqlDB.get_cryostat_press_data(table_prefix=cryopress_table_prefix, tagid=cryopress_tagid)
    subsampled_cryostat_pressure = PsqlDB.get_subsampled_formatted_data(cryostat_pressure, cryopress_variablename, subsample=subsample)
    PsqlDB.dump_to_json(subsampled_cryostat_pressure, cryopress_variablename)

def get_gizmo_ground_tag(influxDB, database, measurement, variables=[], ground_impedance=0.0, ground_impedance_err=0.0, subsample=None):
    if not variables:
        variables = influxDB.fetch_measurement_fields(database, measurement)
    result_data = influxDB.fetch_measurement_data(database, measurement, variables)
    subsampled_data = influxDB.get_subsampled_formatted_data(database, measurement, variables, result_data, subsample=subsample)
    tag = "good ground"
    bad_ground_values = []
    for entry in subsampled_data:
        if 'resistance' in entry:
            if entry['resistance'] < ground_impedance-ground_impedance_err or entry['resistance'] > ground_impedance+ground_impedance_err:
                tag = "bad ground"
                bad_ground_values.append(entry)
        else:
            print("ERROR: No resistance field in data. This function can only be used to calculate good or bad grounding with gizmo")
            return
    if bad_ground_values: print(f"WARNING: Bad grounding detected at these times {bad_ground_values}")

    return tag

def dump_SC_data(influxDB, PsqlDB, config_file, json_filename='', dump_all_data=False):
    config_influx=None
    config_psql=None
    with open(config_file, 'r') as yaml_file:
        config=yaml.safe_load(yaml_file)
    config_influx=config['influxdb']
    config_psql=config['psql']

    data_dict = config_influx.get('influx_SC_data_dict', {})
    if(dump_all_data):
        influx_blind_dump(influxDB, data_dict, subsample=config_influx['subsample_time'])
        psql_blind_dump(PsqlDB,prm_tablename= config_psql['purity_mon_table'], prm_variablename="electron_lifetime",\
            cryopress_table_prefix=config_psql['cryopress_table_prefix'], cryopress_tagid=config_psql['cryopress_tagid'],\
            cryopress_variablename="cryostat_pressure", subsample=config_psql['subsample_time'])
    else:
        ground_tag = get_gizmo_ground_tag(influxDB=influxDB, database="gizmo", measurement="resistance", \
                        ground_impedance=config_influx['ground_impedance'], ground_impedance_err=config_influx['ground_impedance_err'], \
                        subsample=config_influx['subsample_time'])
        electron_lifetime = PsqlDB.get_purity_monitor_data(tablename=config_psql['purity_mon_table'], last_value=True)
        data = {
        "Gizmo grounding": ground_tag,
        "Purity monitor": {
            "last timestamp": electron_lifetime[0].isoformat(),
            "electron lifetime": electron_lifetime[1]
            }
        }
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Data has been dumped into {json_filename}")
