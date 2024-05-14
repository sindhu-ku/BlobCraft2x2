import datetime
import json
import sqlalchemy as dbq
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt

class PsqlDB:

    def __init__(self, config, run_start, run_end, subsample=None):
        print("\n")
        print("**********************************************Querying PostgreSQL Database**********************************************")
        self.config = config
        self.run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
        self.run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
        self.url = self.create_url()
        self.subsample = subsample

    def create_url(self):
        url_template = "postgresql+psycopg2://{username}:{password}@{hostname}/{dbname}"
        return url_template.format(**self.config)

    def get_years_months(self):
        year_st = '{:04d}'.format(self.run_start.year)
        month_st = '{:02d}'.format(self.run_start.month)

        year_end = '{:04d}'.format(self.run_end.year)
        month_end = '{:02d}'.format(self.run_end.month)

        years = [year_st]
        months = [month_st]
        if year_st != year_end:
            years.append(year_end)

        if month_st != month_end:
            months.append(month_end)

        return years, months

    def fetch_data(self):
        run_start_utime = datetime.datetime.timestamp(self.run_start) * 1e3
        run_end_utime = datetime.datetime.timestamp(self.run_end) * 1e3

        years, months = self.get_years_months()

        engine = dbq.create_engine(self.url)
        connection = engine.connect()

        result_data = []

        for y in years:
            for m in months:

                table_name = f'sqlt_data_1_{y}_{m}'

                tab = dbq.table(table_name, dbq.Column("t_stamp"), dbq.Column("floatvalue"), dbq.Column("tagid"))
                query = dbq.select(tab.c.floatvalue, tab.c.t_stamp).select_from(tab).where(dbq.and_(tab.c.tagid == str(self.config['tagid']), tab.c.t_stamp >= str(int(run_start_utime)), tab.c.t_stamp <= str(int(run_end_utime))))

                result = connection.execute(query)
                result_data.extend(result.all())

        self.dump_to_json(result_data)

    def dump_to_json(self, result_data):
        if not result_data:
            print(f"WARNING: No data found for Cryostat pressure for the given time period")
            return

        formatted_data = []
        last_timestamp = None
        subsample_delta = datetime.timedelta(seconds=self.subsample) if self.subsample else None

        for entry in result_data:
            timestamp = datetime.datetime.utcfromtimestamp(entry[1] / 1e3)
            if last_timestamp is None or (subsample_delta and timestamp - last_timestamp >= subsample_delta):
                formatted_data.append({'cryostat pressure': entry[0], 'time': timestamp})
                last_timestamp = timestamp

        def custom_json_serializer(obj):
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()  # Convert datetime to ISO 8601 format
            else:
                return obj  # Return original value for other types

        json_filename = f'cryostat-pressure_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'
        with open(json_filename, 'w') as json_file:
            json.dump(formatted_data, json_file, default=custom_json_serializer, indent=4)

        print(f"Dumping crysostat pressure data to {json_filename}")


class InfluxDBManager:
    def __init__(self, config, run_start, run_end, subsample=None):
        print("\n")
        print("**********************************************Querying InfluxDB Database**********************************************")
        self.config = config
        self.data_dict = config.get('influx_data_dict', {})
        self.run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
        self.run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
        self.client = InfluxDBClient(host=self.config['host'], port=self.config['port'])
        self.subsample = subsample

    def fetch_data(self):
        for database, (measurements, variables) in self.data_dict.items():
            if len(variables) == 1:
                for measurement in measurements:
                    result_data = self.fetch_measurement_data(database, measurement, variables[0])
                    self.dump_to_json(database, measurement, variables[0], result_data)
            else:
                for measurement, measurement_variables in zip(measurements, variables):
                    result_data = self.fetch_measurement_data(database, measurement, measurement_variables)
                    self.dump_to_json(database, measurement, measurement_variables, result_data)

    def fetch_measurement_data(self, database, measurement, variables, subsample=None):
        start_time_ms = int(self.run_start.timestamp() * 1e3)
        end_time_ms = int(self.run_end.timestamp() * 1e3)

        variable_str = ', '.join(variables)
        query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {start_time_ms}ms and time <= {end_time_ms}ms'
        result = self.client.query(query, database=database)
        return result.raw

    def dump_to_json(self, database, measurement, variables, result_data):
        json_filename = f'{database}_{measurement}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'
        series_values = result_data['series'][0]['values'] if 'series' in result_data and result_data['series'] else []

        if not series_values:
            print(f"WARNING: No data found for {variables} in {measurement} from {database}")
            return

        formatted_data = []
        last_timestamp = None
        subsample_delta = datetime.timedelta(seconds=self.subsample) if self.subsample else None


        for entry in series_values:
            timestamp_str = entry[0]
            timestamp = datetime.datetime.fromisoformat(timestamp_str[:-1])
            if last_timestamp is None or (subsample_delta and timestamp - last_timestamp >= subsample_delta):
                formatted_data.append({
                    'time': timestamp_str,
                    **{variables[i]: entry[i + 1] for i in range(len(entry) - 1)}
                    })
                last_timestamp = timestamp

        with open(json_filename, 'w') as json_file:
            json.dump(formatted_data, json_file, indent=4)
            print(f"Dumping {variables} from {measurement} from {database} to {json_filename}")
