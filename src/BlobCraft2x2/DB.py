#!/usr/bin/env python3

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sqlalchemy as alc
import sqlite3
from influxdb import InfluxDBClient
import requests
import pandas as pd

chicago_tz =  ZoneInfo("America/Chicago")

class PsqlDBManager:
    def __init__(self, config):
        self.config = config
        self.start = None
        self.end = None
        self.url = self.create_url()
        self.engine = alc.create_engine(self.url)
        self.connection = self.engine.connect()

    def set_time_range(self, start, end):
        self.start = start
        self.end = end

    def create_url(self):
        url_template = "postgresql+psycopg2://{username}:{password}@{hostname}/{dbname}"
        return url_template.format(**self.config)

    def get_years_months(self):
        year_st = "{:04d}".format(self.start.year)
        month_st = "{:02d}".format(self.start.month)

        year_end = "{:04d}".format(self.end.year)
        month_end = "{:02d}".format(self.end.month)

        years = [year_st]
        months = [month_st]
        if year_st != year_end:
            years.append(year_end)

        if month_st != month_end:
            months.append(month_end)

        return years, months

    def get_cryostat_data(self, table_prefix, variable, tagid):
        print(f"\nQuerying {variable} data from PostgreSQL Database")

        result_data = []
        years, months = self.get_years_months() # we have to do this because cryostat data is split into multiple tables based on years and months. This should account for runs spanning across months or yeara
        start_utime = int(self.start.timestamp() * 1e3)
        end_utime = int(self.end.timestamp() * 1e3)
        for y in years:
            for m in months:
                table_name = f"{table_prefix}_{y}_{m}"
                tab = alc.table(table_name, alc.Column("t_stamp"), alc.Column("floatvalue"), alc.Column("tagid"))
                query = alc.select(tab.c.t_stamp, tab.c.floatvalue).select_from(tab).where(alc.and_(tab.c.tagid == str(tagid), tab.c.t_stamp >= str(int(start_utime)), tab.c.t_stamp <= str(int(end_utime))))
                result = self.connection.execute(query)
                result_data.extend(result.all())
        return result_data

    def get_purity_monitor_data(self, tablename, variables=[], last_value=False):
        print(f"\nQuerying {variables} from purity monitor measurements from PostgreSQL Database")

        result_data = []
        columns = [alc.Column("timestamp")] + [alc.Column(var) for var in variables]
        tab = alc.table(tablename, *columns)

        query_columns = [tab.c.timestamp] + [tab.c[var] for var in variables]

        if last_value:
            day_increment = timedelta(days=1)
            while not result_data:
                initial_start = self.start
                query = alc.select(*query_columns).select_from(tab).where(alc.and_(tab.c.timestamp >= self.start, tab.c.timestamp <= self.end))
                result = self.connection.execute(query)
                result_data.extend(result.all())

                if result_data:
                    return result_data[-1]
                else:
                    print(f"WARNING: No data found for the given time period {self.start} to {self.end}")
                    self.start = initial_start - day_increment
                    self.end = initial_start
                    print(f"Extending time range by one day before the subrun: {self.start} to {self.end} to obtain the last purity monitor measurement")
        else:
            query = alc.select(*query_columns).select_from(tab).where(alc.and_(tab.c.timestamp >= self.start, tab.c.timestamp <= self.end))
            result = self.connection.execute(query)
            result_data.extend(result.all())

            if result_data:
                return result_data
            else:
                print(f"WARNING: No data found for the given time period from {self.start} to {self.end}")
                return result_data

    def make_filename(self, measurement_name):
        return f"{measurement_name}_{self.start.isoformat()}_{self.end.isoformat()}"


    def close_connection(self):
        if self.connection is not None:
            self.connection.close()

class InfluxDBManager:
    def __init__(self, config):
        self.config = config
        self.start = None
        self.end = None
        self.client = InfluxDBClient(host=self.config["host"], port=self.config["port"])

    def set_time_range(self, start, end):
        self.start = start
        self.end = end

    def fetch_measurement_fields(self, database, measurement):
        result = self.client.query(f'SHOW FIELD KEYS ON "{database}" FROM "{measurement}"')
        fields = [field["fieldKey"] for field in result.get_points()]
        return fields

    def fetch_measurements(self, database):
        query = f'SHOW MEASUREMENTS ON "{database}"'
        result = self.client.query(query)
        measurements = [measurement["name"] for measurement in result.get_points()]
        return measurements

    def fetch_measurement_data(self, database, measurement, variables=[], subsample=None):
        print(f"\nQuerying {variables} in {measurement} from {database} from InfluxDB Database")

        start_utime = int(self.start.timestamp() * 1e3)
        end_utime = int(self.end.timestamp() * 1e3)

        query = ''
        variable_str = ', '.join(variables)

        tag_keys = self.fetch_tag_keys(database, measurement)
        tag_keys_str = ', '.join(tag_keys)

        if tag_keys: query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {start_utime}ms and time <= {end_utime}ms GROUP BY {tag_keys_str}'
        else:  query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {start_utime}ms and time <= {end_utime}ms'
        result = self.client.query(query, database=database)
        return result

    def fetch_tag_keys(self, database, measurement):
        tag_keys_result = self.client.query(f'SHOW TAG KEYS ON "{database}" FROM "{measurement}"')
        tag_keys = [tag["tagKey"] for tag in tag_keys_result.get_points()]
        return tag_keys

    def make_filename(self, database, measurement):
        return f'{database}_{measurement}_{self.start.isoformat()}_{self.end.isoformat()}'

    def close_connection(self):
        if self.client is not None:
            self.client.close()

class SQLiteDBManager:
    def __init__(self, filename, run):
        self.run = run
        self.filename = filename
        self.conn = sqlite3.connect(self.filename)
        self.cursor = self.conn.cursor()

    def query_data(self, table_name, conditions, columns):
        columns_str = ", ".join(columns)
        condition_str = " AND ".join(conditions)
        query = f"SELECT {columns_str} FROM {table_name} WHERE {condition_str}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def get_column_names(self, table_name):
        query = f"PRAGMA table_info({table_name})"
        self.cursor.execute(query)
        columns_info = self.cursor.fetchall()
        column_names = [info[1] for info in columns_info]  # The second element in each tuple is the column name
        return column_names

    def get_subruns(self, table, start, end, subrun, condition):
        subrun_columns = [subrun, start, end]
        subruns_data = self.query_data(table_name=table, conditions=[f"{condition}={self.run}"], columns=subrun_columns)

        subruns = {}
        for row in subruns_data:
            subrun_info = dict(zip(subrun_columns, row))
            subrun_number = subrun_info[subrun] %10000
            subrun_times = {
                'start_time': datetime.fromtimestamp(subrun_info[start], tz=chicago_tz).isoformat(),
                'end_time': datetime.fromtimestamp(subrun_info[end], tz=chicago_tz).isoformat()
            }
            subruns[subrun_number] = subrun_times

        return subruns

    def extract_schema(self, data):
        schema = {}
        for subrun, details in data.items():
            for key, value in details.items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        composite_key = f"{key}_{subkey}"
                        if composite_key not in schema:
                            schema[composite_key] = type(subvalue)
                else:
                    if key not in schema:
                        schema[key] = type(value)
        return schema

    def create_table(self, table_name, schema):
        columns = []
        for col, col_type in schema.items():
            if col_type == int:
                col_type = "INTEGER"
            elif col_type == float:
                col_type = "REAL"
            else:
                col_type = "TEXT"
            columns.append(f"{col} {col_type}")
        columns_str = ", ".join(columns)
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (subrun TEXT PRIMARY KEY, {columns_str})")

    def insert_data(self, table_name, data):
        for subrun, details in data.items():
            flat_details = {"subrun": subrun}
            for key, value in details.items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        composite_key = f"{key}_{subkey}"
                        flat_details[composite_key] = subvalue
                else:
                    flat_details[key] = value

            columns = ", ".join(flat_details.keys())
            placeholders = ", ".join(["?"] * len(flat_details))
            values = list(flat_details.values())

            self.cursor.execute(f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})", values)

    def dump_data(self, data, table_name):
        schema = self.extract_schema(data)
        self.create_table(table_name, schema)
        self.insert_data(table_name, data)
        self.conn.commit()

    def close_connection(self):
        if self.conn:
            self.conn.close()



class IFBeamManager:
    def __init__(self, config):
        self.config = config
        self.start = None
        self.end = None

    def set_time_range(self, start, end):
        self.start = start
        self.end = end

    def make_url(self, device_name):
        base_url = f"https://{self.config['url']}v={device_name}&e={self.config['event']}&t0={self.start}&t1={self.end}&f=json"
        return base_url

    def fetch_data(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f'An error occurred: {e}')
            return None

    def get_data(self, device_name, combine_unit=False):
        url = self.make_url(device_name)
        data = self.fetch_data(url)
        if data:
            return self.extract_time_series(data, combine_unit=combine_unit)
        else:
            print(f"WARNING: No beam data found for {self.start} to {self.end}!")
            return {}

    def extract_time_series(self, data, combine_unit=False):
        if 'rows' in data:
            rows = data['rows']
        else:
            raise ValueError('No data rows found in beam data')

        df_timeseries = pd.DataFrame(rows)
        if df_timeseries.empty:
            print(f"WARNING: No beam data found from {self.start} to {self.end}!")
            return df_timeseries

        if combine_unit:
            df_timeseries['value'] = df_timeseries.apply(lambda row: f"{row['value']}{row['units']}", axis=1)
            df_timeseries = df_timeseries[['time', 'value']]
        else:
            df_timeseries = df_timeseries[['time', 'value', 'unit']]

        df_timeseries['value'] = pd.to_numeric(df_timeseries['value'], errors='coerce')

        return df_timeseries
