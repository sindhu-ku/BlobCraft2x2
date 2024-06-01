import datetime
import sqlalchemy as dbq
from influxdb import InfluxDBClient

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
        year_st = "{:04d}".format(self.run_start.year)
        month_st = "{:02d}".format(self.run_start.month)

        year_end = "{:04d}".format(self.run_end.year)
        month_end = "{:02d}".format(self.run_end.month)

        years = [year_st]
        months = [month_st]
        if year_st != year_end:
            years.append(year_end)

        if month_st != month_end:
            months.append(month_end)

        return years, months

    def get_cryostat_data(self, table_prefix, variable, tagid):
        print("\n")
        print(f"**********************************************Querying {variable} data from PostgreSQL Database**********************************************")

        result_data = []
        years, months = self.get_years_months()
        run_start_utime = int(self.run_start.timestamp() * 1e3)
        run_end_utime = int(self.run_end.timestamp() * 1e3)
        for y in years:
            for m in months:
                table_name = f"{table_prefix}_{y}_{m}"
                tab = dbq.table(table_name, dbq.Column("t_stamp"), dbq.Column("floatvalue"), dbq.Column("tagid"))
                query = dbq.select(tab.c.t_stamp, tab.c.floatvalue).select_from(tab).where(dbq.and_(tab.c.tagid == str(tagid), tab.c.t_stamp >= str(int(run_start_utime)), tab.c.t_stamp <= str(int(run_end_utime))))
                result = self.connection.execute(query)
                result_data.extend(result.all())
        return result_data

    def get_purity_monitor_data(self, tablename, variables=[], last_value=False):
        print("\n")
        print(f"**********************************************Querying {variables} from purity monitor measurements from PostgreSQL Database**********************************************")

        result_data = []
        columns = [dbq.Column("timestamp")] + [dbq.Column(var) for var in variables]
        tab = dbq.table(tablename, *columns)

        query_columns = [tab.c.timestamp] + [tab.c[var] for var in variables]
        query = dbq.select(*query_columns).select_from(tab).where(dbq.and_(tab.c.timestamp >= self.run_start, tab.c.timestamp <= self.run_end))

        result = self.connection.execute(query)
        result_data.extend(result.all())
        if last_value:
            if result_data:
                return result_data[-1]
            else:
                print(f"WARNING: No data found for the given time period")
                return self.run_start, {var: 0.0 for var in variables}
        else:
            return result_data

    def make_filename(self, measurement_name):
        return f"{measurement_name}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json"



class InfluxDBManager:
    def __init__(self, config, run_start, run_end):
        self.config = config
        self.run_start = run_start
        self.run_end = run_end
        self.client = InfluxDBClient(host=self.config["host"], port=self.config["port"])

    def fetch_measurement_fields(self, database, measurement):
        result = self.client.query(f'SHOW FIELD KEYS ON "{database}" FROM "{measurement}"')
        fields = [field["fieldKey"] for field in result.get_points()]
        return fields

    def fetch_measurement_data(self, database, measurement, variables, subsample=None):
        print("\n")
        print(f"**********************************************Querying {variables} in {measurement} from {database} from InfluxDB Database**********************************************")
        run_start_utime = int(self.run_start.timestamp() * 1e3)
        run_end_utime = int(self.run_end.timestamp() * 1e3)
        query = ''
        variable_str = ', '.join(variables)

        tag_keys = self.fetch_tag_keys(database, measurement)
        tag_keys_str = ', '.join(tag_keys)

        if tag_keys: query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {run_start_utime}ms and time <= {run_end_utime}ms GROUP BY {tag_keys_str}'
        else:  query = f'SELECT {variable_str} FROM "{measurement}" WHERE time >= {run_start_utime}ms and time <= {run_end_utime}ms'
        result = self.client.query(query, database=database)
        return result

    def fetch_tag_keys(self, database, measurement):
        tag_keys_result = self.client.query(f'SHOW TAG KEYS ON "{database}" FROM "{measurement}"')
        tag_keys = [tag["tagKey"] for tag in tag_keys_result.get_points()]
        return tag_keys

    def make_filename(self, database, measurement):
        return f'{database}_{measurement}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'
