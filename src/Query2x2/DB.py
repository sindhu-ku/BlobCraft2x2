#!/usr/bin/env python3

from datetime import datetime
from zoneinfo import ZoneInfo
import sqlalchemy as alc
import sqlite3
from influxdb import InfluxDBClient

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
        years, months = self.get_years_months()
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
        query = alc.select(*query_columns).select_from(tab).where(alc.and_(tab.c.timestamp >= self.start, tab.c.timestamp <= self.end))

        result = self.connection.execute(query)
        result_data.extend(result.all())
        if last_value:
            if result_data:
                return result_data[-1]
            else:
                print(f"WARNING: No data found for the given time period")
                return self.start, 0.0
        else:
            return result_data

    def make_filename(self, measurement_name):
        return f"{measurement_name}_{self.start.isoformat()}_{self.end.isoformat()}.json"


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
        return f'{database}_{measurement}_{self.start.isoformat()}_{self.end.isoformat()}.json'

    def close_connection(self):
        if self.client is not None:
            self.client.close()

class SQLiteManager:
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

    def get_subruns(self):
        subrun_columns = ['subrun', 'start_time_unix', 'end_time_unix']
        subruns_data = self.query_data(table_name='lrs_runs_data', conditions=[f"morcs_run_nr=={self.run}"], columns=subrun_columns)

        subruns = {}
        for row in subruns_data:
            subrun_info = dict(zip(subrun_columns, row))
            subrun_number = subrun_info['subrun']
            subrun_times = {
                'start_time': datetime.fromtimestamp(subrun_info['start_time_unix'], chicago_tz),
                'end_time': datetime.fromtimestamp(subrun_info['end_time_unix'], chicago_tz)
            }
            subruns[subrun_number] = subrun_times

        return subruns

    def get_moas_version_data(self, moas_filename, moas_columns):
        moas_version = moas_filename[5:-4]

        moas_data = self.query_data(table_name='moas_versions', conditions=[f"version=='{moas_version}'"], columns=moas_columns)

        if not moas_data:
            raise ValueError(f"ERROR: No data found for MOAS version extracted from filename: {moas_filename}")
        if len(moas_data) > 1:
            raise ValueError(f"ERROR: Multiple MOAS versions found for version {moas_version}")
        return [dict(zip(moas_columns, row)) for row in moas_data]

    def get_moas_channels_data(self, config_id, moas_channels_columns):
        moas_channels_data = self.query_data(table_name='moas_channels', conditions=[f"config_id=={config_id}"], columns=moas_channels_columns)
        if not moas_channels_data:
            raise ValueError(f"ERROR: No MOAS channels data found")
        return [dict(zip(moas_channels_columns, row)) for row in moas_channels_data]

    def close_connection(self):
        if self.conn:
            self.conn.close()
