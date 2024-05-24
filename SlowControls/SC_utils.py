def blind_dump(influxDB, influx_data_dict):
    for database, (measurements, variables) in data_dict.items():
        if not variables:
            variables = [influxDB.fetch_measurement_fields(database, measurement) for measurement in measurements]
        for measurement, measurement_variables in zip(measurements, variables):
            result_data = influxDB.fetch_measurement_data(database, measurement, measurement_variables)
            formatted_data = influxDB.get_subsampled_formatted_data(database, measurement, measurement_variables, result_data)
            influxDB.dump_to_json(formatted_data, database, measurement)

def dump_gizmo_grounding(influxDB, database, measurement, variables=[]):
    if not variables:
        variables = influxDB.fetch_measurement_fields(database, measurement)
    result_data = influxDB.fetch_measurement_data(database, measurement, variables)
    subsampled_data = influxDB.get_subsampled_formatted_data(database, measurement, variables, result_data)
    formatted_data = influxDB.calc_grounding(subsampled_data)
    influxDB.dump_to_json(formatted_data, database, measurement)


def dump_SC_data(influxDB, config, dump_all_data=False):

    data_dict = config.get('influx_SC_data_dict', {})
    if(dump_all_data): blind_dump(influxDB, data_dict)
    else:
        dump_gizmo_grounding(influxDB=influxDB, database="gizmo", measurement="resistance")
