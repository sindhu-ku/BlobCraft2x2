import datetime
import json
import sqlalchemy as dbq
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import pandas as pd
import pytz

class PsqlDB:

    def __init__(self, config, run_start, run_end, subsample=None):
        print("\n")
        print("**********************************************Querying PostgreSQL Database**********************************************")
        self.config = config
        self.run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
        self.run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
        self.url = self.create_url()
        self.engine = dbq.create_engine(self.url)
        self.connection = self.engine.connect()
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

    def fetch_data(self, variable):
        # inspector = dbq.inspect(self.engine)
        # tables = inspector.get_table_names()
        # print("Available tables:")
        # for table in tables:
        #     print(table)
        #
        # table_name = 'prm_table'
        # columns = inspector.get_columns(table_name)
        # column_names = [column['name'] for column in columns]
        # print("Column Names:", column_names)

        if variable=="cryostat_pressure":
            result_data = self.get_cryostat_press_data()
        elif variable=="electron_lifetime":
            result_data = self.get_purity_monitor_data()
        else:
            print("ERROR: Cannot access {variable}. I can only currently handle 'cryostat_pressure' and 'electron_lifetime'")
            return
        formatted_data = self.get_subsampled_formatted_data(result_data, variable)
        self.dump_to_json(formatted_data, variable)

    def get_data(self, table_name, columns, conditions):
        result_data = []
        tab = dbq.table(table_name, *columns)
        query = dbq.select(*[col for col in columns]).select_from(tab).where(conditions)
        result = self.connection.execute(query)
        result_data.extend(result.all())
        return result_data

    def get_cryostat_press_data(self):
        result_data = []
        years, months = self.get_years_months()
        run_start_utime = datetime.datetime.timestamp(self.run_start) * 1e3
        run_end_utime = datetime.datetime.timestamp(self.run_end) * 1e3
        for y in years:
            for m in months:
                table_name = f"{self.config['cryopress_table_prefix']}_{y}_{m}"
                tab = dbq.table(table_name, dbq.Column("t_stamp"), dbq.Column("floatvalue"), dbq.Column("tagid"))
                query = dbq.select(tab.c.t_stamp, tab.c.floatvalue).select_from(tab).where(dbq.and_(tab.c.tagid == str(self.config['tagid']), tab.c.t_stamp >= str(int(run_start_utime)), tab.c.t_stamp <= str(int(run_end_utime))))
                result = self.connection.execute(query)
                result_data.extend(result.all())
        return result_data

    def get_purity_monitor_data(self):
        result_data= []
        table_name = str(self.config['purity_mon_table'])
        tab = dbq.table(table_name, dbq.Column("timestamp"), dbq.Column("prm_lifetime"))
        query = dbq.select(tab.c.timestamp, tab.c.prm_lifetime).select_from(tab).where(dbq.and_(tab.c.timestamp >= self.run_start, tab.c.timestamp <= self.run_end))
        result = self.connection.execute(query)
        result_data.extend(result.all())
        return result_data

    def get_subsampled_formatted_data(self, data, variable):

        if not data:
            print(f"WARNING: No data found for {variable} for the given time period")
            return

        df = pd.DataFrame(data, columns=['time', variable])
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        if self.subsample is not None:
            df_resampled = df.set_index('time').resample(self.subsample).mean().dropna().reset_index()
            result_data = df_resampled.values.tolist()
        else:
            result_data = df.values.tolist()

        formatted_data = [{variable: entry[1], 'time': pd.to_datetime(entry[0], unit='ms', utc=True).isoformat()} for entry in result_data]
        return formatted_data

    def dump_to_json(self, formatted_data, variable):

        json_filename = f'{variable}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'
        with open(json_filename, 'w') as json_file:
            json.dump(formatted_data, json_file, indent=4)

        print(f"Dumping {variable} data to {json_filename}")



class InfluxDBManager:
    def __init__(self, config, run_start, run_end, subsample=None):
        print("\n")
        print("**********************************************Querying InfluxDB Database**********************************************")
        self.config = config
        self.run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
        self.run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
        self.client = InfluxDBClient(host=self.config['host'], port=self.config['port'])
        self.subsample = subsample

    def fetch_measurement_fields(self, database, measurement):
        result = self.client.query(f'SHOW FIELD KEYS ON "{database}" FROM "{measurement}"')
        fields = [field['fieldKey'] for field in result.get_points()]
        return fields

    def fetch_measurement_data(self, database, measurement, variables, subsample=None):
        start_time_ms = int(self.run_start.timestamp() * 1e3)
        end_time_ms = int(self.run_end.timestamp() * 1e3)

        query = ''
        variable_str = ', '.join(variables)

        tag_keys = self.fetch_tag_keys(database, measurement)
        tag_keys_str = ', '.join(tag_keys)

        if tag_keys: query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {start_time_ms}ms and time <= {end_time_ms}ms GROUP BY {tag_keys_str}'
        else:  query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {start_time_ms}ms and time <= {end_time_ms}ms'
        result = self.client.query(query, database=database)
        return result

    def fetch_tag_keys(self, database, measurement):
        tag_keys_result = self.client.query(f'SHOW TAG KEYS ON "{database}" FROM "{measurement}"')
        tag_keys = [tag['tagKey'] for tag in tag_keys_result.get_points()]
        return tag_keys

    def get_subsampled_formatted_data(self, database, measurement, variables, result_data):

        if not result_data:
            print(f"WARNING: No data found for {variables} in {measurement} from {database}")
            return

        formatted_data = []
        for key, data_points in result_data.items():
            measurement_name, tags_dict = key
            df = pd.DataFrame(data_points)
            df['time'] = pd.to_datetime(df['time'])

            if self.subsample is not None:
                df_resampled = df.resample(self.subsample, on='time').mean().dropna()
                resampled_entries = df_resampled.reset_index().to_dict('records')
            else:
                resampled_entries = df.to_dict('records')

            for entry in resampled_entries:
                formatted_entry = {
                'time': entry['time'].isoformat(),
                **{var: entry[var] for var in variables}
                }
                if tags_dict:
                    formatted_entry['tags'] = tags_dict

                formatted_data.append(formatted_entry)
        return formatted_data

    def calc_grounding(self, formatted_data):
        ground_impedance = self.config['ground_impedance']
        ground_impedance_err = self.config['ground_impedance_err']
        for entry in formatted_data:
            if 'resistance' in entry:
                if (ground_impedance-ground_impedance_err) <= entry['resistance'] <= (ground_impedance+ground_impedance_err):
                    entry['ground_status'] = "good detector ground"
                else:
                    entry['ground_status'] = "bad detector ground"
            else:
                print("ERROR: NO resistance field in data. This function can only be used to calculate good or bad grounding with gizmo")
        return formatted_data

    def dump_to_json(self, data, database, measurement):
        json_filename = f'{database}_{measurement}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'

        with open(json_filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)

        print(f"Dumping data to {json_filename}")
