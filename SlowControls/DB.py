import json
import datetime
import sqlalchemy as dbq
from influxdb import InfluxDBClient
import pandas as pd

class PsqlDBManager:

    def __init__(self, config, run_start, run_end):

        self.config = config
        self.run_start = run_start
        self.run_end = run_end
        self.url = self.create_url()
        self.engine = dbq.create_engine(self.url)
        self.connection = self.engine.connect()

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

    def get_cryostat_data(self, table_prefix, tagid):
        print("\n")
        print("**********************************************Querying Cryostat data from PostgreSQL Database**********************************************")
        result_data = []
        years, months = self.get_years_months()
        run_start_utime = datetime.datetime.timestamp(self.run_start) * 1e3
        run_end_utime = datetime.datetime.timestamp(self.run_end) * 1e3
        for y in years:
            for m in months:
                table_name = f"{table_prefix}_{y}_{m}"
                tab = dbq.table(table_name, dbq.Column("t_stamp"), dbq.Column("floatvalue"), dbq.Column("tagid"))
                query = dbq.select(tab.c.t_stamp, tab.c.floatvalue).select_from(tab).where(dbq.and_(tab.c.tagid == str(tagid), tab.c.t_stamp >= str(int(run_start_utime)), tab.c.t_stamp <= str(int(run_end_utime))))
                result = self.connection.execute(query)
                result_data.extend(result.all())
        return result_data

    def get_purity_monitor_data(self, tablename, last_value=False):
        print("\n")
        print("**********************************************Querying Purity monitor electron lifetime from PostgreSQL Database**********************************************")
        result_data= []
        tab = dbq.table(tablename, dbq.Column("timestamp"), dbq.Column("prm_lifetime"))
        query = dbq.select(tab.c.timestamp, tab.c.prm_lifetime).select_from(tab).where(dbq.and_(tab.c.timestamp >= self.run_start, tab.c.timestamp <= self.run_end))
        result = self.connection.execute(query)
        result_data.extend(result.all())
        if last_value:
            if result_data:
                return result_data[len(result_data)-1]
            else:
                print(f"WARNING: No data found for the given time period")
                return(self.run_start, 0.0)
        else:
            return result_data

    def get_subsampled_formatted_data(self, data, variable, subsample=None):
        if not data:
            print(f"WARNING: No data found for {variable} for the given time period")
            return

        df = pd.DataFrame(data, columns=['time', variable])
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        if subsample is not None:
            df_resampled = df.set_index('time').resample(subsample).mean().dropna().reset_index()
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
    def __init__(self, config, run_start, run_end):
        self.config = config
        self.run_start = run_start
        self.run_end = run_end
        self.client = InfluxDBClient(host=self.config['host'], port=self.config['port'])

    def fetch_measurement_fields(self, database, measurement):
        result = self.client.query(f'SHOW FIELD KEYS ON "{database}" FROM "{measurement}"')
        fields = [field['fieldKey'] for field in result.get_points()]
        return fields

    def fetch_measurement_data(self, database, measurement, variables, subsample=None):
        print("\n")
        print(f"**********************************************Querying {variables} in {measurement} from {database} from InfluxDB Database**********************************************")
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

    def get_subsampled_formatted_data(self, database, measurement, variables, result_data, subsample=None):

        if not result_data:
            print(f"WARNING: No data found for {variables} in {measurement} from {database}")
            return

        formatted_data = []
        for key, data_points in result_data.items():
            measurement_name, tags_dict = key
            df = pd.DataFrame(data_points)
            df['time'] = pd.to_datetime(df['time'])

            if subsample is not None:
                df_resampled = df.resample(subsample, on='time').mean().dropna()
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

    def dump_to_json(self, data, database, measurement):
        json_filename = f'{database}_{measurement}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'

        with open(json_filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)

        print(f"Dumping data to {json_filename}")
