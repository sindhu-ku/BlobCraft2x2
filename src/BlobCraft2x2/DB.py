#!/usr/bin/env python3

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sqlalchemy as alc
import sqlite3
from influxdb import InfluxDBClient
import numpy as np
import requests
import pandas as pd
from . import local_tz, Global_config

# Connection timeout (s) for network databases; reads are not limited, so long queries still finish
connect_timeout = Global_config.get('connect_timeout', 20)

class PsqlDBManager:
    def __init__(self, config):
        self.config = config
        self.start = None
        self.end = None
        self.url = self.create_url()
        self.engine = alc.create_engine(self.url, connect_args={"connect_timeout": connect_timeout})
        self._connection = None

    @property
    def connection(self):
        # Connect on first use so queries that only need InfluxDB never touch PostgreSQL
        if self._connection is None:
            self._connection = self.engine.connect()
        return self._connection

    def set_time_range(self, start, end):
        self.start = start
        self.end = end

    def create_url(self):
        url_template = "postgresql+psycopg2://{username}:{password}@{hostname}/{dbname}"
        return url_template.format(**self.config)

    def get_years_months(self):
        year, month = self.start.year, self.start.month
        years_months = []
        while (year, month) <= (self.end.year, self.end.month):
            years_months.append(("{:04d}".format(year), "{:02d}".format(month)))
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)
        return years_months

    def get_cryostat_data(self, table_prefix, variable, tagid):
        print(f"\nQuerying {variable} data from PostgreSQL Database")

        result_data = []
        years_months = self.get_years_months() # we have to do this because cryostat data is split into multiple tables based on years and months. This should account for runs spanning across months or years
        start_utime = int(self.start.timestamp() * 1e3)
        end_utime = int(self.end.timestamp() * 1e3)
        for y, m in years_months:
            table_name = f"{table_prefix}_{y}_{m}"
            tab = alc.table(table_name, alc.Column("t_stamp"), alc.Column("floatvalue"), alc.Column("tagid"))
            query = alc.select(tab.c.t_stamp, tab.c.floatvalue).select_from(tab).where(alc.and_(tab.c.tagid == str(tagid), tab.c.t_stamp >= str(int(start_utime)), tab.c.t_stamp <= str(int(end_utime)))).order_by(tab.c.t_stamp)
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


    def get_dcs_data(self, channel):
        print(f"\nQuerying {channel} from dcs_prd archiver in PostgreSQL Database")

        # smpl_time is stored as naive local time
        start = self.start.astimezone(local_tz).replace(tzinfo=None)
        end = self.end.astimezone(local_tz).replace(tzinfo=None)
        query = alc.text("SELECT s.smpl_time, COALESCE(s.float_val, s.num_val) FROM dcs_prd.sample s "
                         "JOIN dcs_prd.channel c ON c.channel_id = s.channel_id "
                         "WHERE c.name = :channel AND s.smpl_time >= :start AND s.smpl_time <= :end ORDER BY s.smpl_time")
        result = self.connection.execute(query, {"channel": channel, "start": start, "end": end})
        return [(int(t.replace(tzinfo=local_tz).timestamp() * 1e3), val) for t, val in result.all()] # ms, like the cryostat tables

    def get_dcs_channels(self, pattern):
        print(f"\nQuerying channels matching {pattern} from dcs_prd archiver in PostgreSQL Database")

        # Channels archived on change (e.g. MPOD) may have no sample in the time range, so also take each
        # channel's last value before the start, at the start time. smpl_time is stored as naive local time
        start = self.start.astimezone(local_tz).replace(tzinfo=None)
        end = self.end.astimezone(local_tz).replace(tzinfo=None)
        query = alc.text("WITH ch AS (SELECT channel_id, name FROM dcs_prd.channel WHERE name ~ :pattern) "
                         "SELECT ch.name, s.smpl_time, COALESCE(s.float_val, s.num_val) FROM ch JOIN dcs_prd.sample s ON s.channel_id = ch.channel_id "
                         "WHERE s.smpl_time >= :start AND s.smpl_time <= :end "
                         "UNION ALL "
                         "SELECT ch.name, CAST(:start AS timestamp), last.val FROM ch CROSS JOIN LATERAL "
                         "(SELECT COALESCE(float_val, num_val) AS val FROM dcs_prd.sample WHERE channel_id = ch.channel_id AND smpl_time < :start "
                         "ORDER BY smpl_time DESC LIMIT 1) last")
        result = self.connection.execute(query, {"pattern": pattern, "start": start, "end": end})
        return [(name, t.replace(tzinfo=local_tz), val) for name, t, val in result.all()]

    def close_connection(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

class InfluxDBManager:
    def __init__(self, config):
        self.config = config
        self.start = None
        self.end = None
        self.client = InfluxDBClient(**config, timeout=(connect_timeout, None))

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
        # quote plain field names (some are InfluxQL keywords, e.g. "drop"); expressions like mean("ch-1") are used as given
        variable_str = ', '.join(var if '(' in var or '"' in var else f'"{var}"' for var in variables)

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

            # assume that any subrun with an invalid timestamp is "bad"
            if (subrun_info[start] in [0, -1]) or (subrun_info[end] in [0, -1]):
                continue

            subrun_number = subrun_info[subrun] %10000
            subrun_times = {
                'run': self.run,
                'start_time': datetime.fromtimestamp(int(subrun_info[start]), tz=local_tz).isoformat(),
                'end_time': datetime.fromtimestamp(int(subrun_info[end]), tz=local_tz).isoformat()
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

    def create_table(self, table_name, schema, is_global_subrun=False):
        columns = []
        for col, col_type in schema.items():
            if col.lower() == 'run':
                continue
            if any(issubclass(col_type, c) for c in [int, np.integer]):
                col_type = "INTEGER"
            elif any(issubclass(col_type, c) for c in [float, np.floating]):
                col_type = "REAL"
            else:
                col_type = "TEXT"
            columns.append(f"{col} {col_type}")
        columns_str = ", ".join(columns)
        if is_global_subrun:
            key_cols = 'global_subrun INTEGER'
            key_str = 'PRIMARY KEY (global_subrun)'
        else:
            key_cols = 'run INTEGER, subrun INTEGER'
            key_str = 'PRIMARY KEY (run, subrun)'
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({key_cols}, {columns_str}, {key_str})")

    def insert_data(self, table_name, data, global_run=None, is_global_subrun=False):
        for subrun, details in data.items():
            flat_details = {}
            if is_global_subrun:
                flat_details['global_subrun'] = subrun
            else:
                flat_details['run'] = details['run']
                flat_details['subrun'] = subrun
            flat_details['global_run'] = global_run

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

    def dump_data(self, data, table_name, global_run=None, is_global_subrun=False):
        schema = self.extract_schema(data)
        self.create_table(table_name, schema, is_global_subrun=is_global_subrun)
        self.insert_data(table_name, data, global_run=global_run, is_global_subrun=is_global_subrun)
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
            response = requests.get(url, timeout=(connect_timeout, None))
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
            return pd.DataFrame()

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
