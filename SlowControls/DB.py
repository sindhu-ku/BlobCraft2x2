import datetime
import yaml
import json
import sqlalchemy as dbq
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt

class PsqlDB:

    def __init__(self, config_yaml, run_start, run_end):
        print("\n")
        print("**********************************************Querying PostgreSQL Database**********************************************")
        self.config_yaml = config_yaml
        self.run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
        self.run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
        self.config = self.load_config()
        self.url = self.create_url()

    def load_config(self):
        with open(self.config_yaml, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)

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

        return result_data

    def dump_to_json(self, result_data):
        formatted_data = [{'cryostat pressure': entry[0], 'timestamp': datetime.datetime.utcfromtimestamp(entry[1] / 1e3)} for entry in result_data]

        def custom_json_serializer(obj):
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()  # Convert datetime to ISO 8601 format
            else:
                return obj  # Return original value for other types

        json_filename = f'cryostat-pressure_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'
        with open(json_filename, 'w') as json_file:
            json.dump(formatted_data, json_file, default=custom_json_serializer, indent=4)

        print(f"Dumping crysostat pressure data to {json_filename}")

    def plot_cryo_pressure(self, result_data):
        timestamps = [datetime.datetime.utcfromtimestamp(entry[1] / 1e3) for entry in result_data]
        pressures = [entry[0]*1e3 for entry in result_data]

        plt.plot(timestamps, pressures)
        plt.xlabel('Time')
        plt.ylabel('Cryostat Pressure (mbar)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pltname = f'cryostat-pressure_{self.run_start.isoformat()}_{self.run_end.isoformat()}.png'
        plt.savefig(pltname)
        plt.close()

        print(f"Crysostat pressure data plotted and saved as {pltname}")

class InfluxDBManager:

    def __init__(self, config_yaml, database, variable, run_start, run_end):
        print("\n")
        print("**********************************************Querying InfluxDB Database**********************************************")
        self.config_yaml = config_yaml
        self.database = database
        self.variable = variable
        self.run_start = datetime.datetime.strptime(run_start, '%Y-%m-%d %H:%M:%S.%f')
        self.run_end = datetime.datetime.strptime(run_end, '%Y-%m-%d %H:%M:%S.%f')
        self.host, self.port = self.load_config()
        self.client = InfluxDBClient(host=self.host, port=self.port, database=self.database)

    def load_config(self):
        with open(self.config_yaml, 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)
            return config['host'], config['port']

    def fetch_data(self):
        start_time_ms = self.run_start.timestamp() * 1e3
        end_time_ms = self.run_end.timestamp() * 1e3

        query = f"SELECT {self.variable} FROM {self.variable} WHERE time >= {int(start_time_ms)}ms and time <= {int(end_time_ms)}ms"
        result = self.client.query(query)
        return result.raw

    def dump_to_json(self, result_data):

        json_filename = f'{self.database}-{self.variable}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.json'
        formatted_data = [{f'{self.variable}': entry[1], 'timestamp': entry[0]} for entry in result_data['series'][0]['values']]

        with open(json_filename, 'w') as json_file:
            json.dump(formatted_data, json_file, indent=4)

        print(f"Dumping {self.variable} from {self.database} to {json_filename}")

    def plot_data(self, result_data):
        timestamps = [datetime.datetime.strptime(entry[0], "%Y-%m-%dT%H:%M:%SZ") for entry in result_data['series'][0]['values']]
        values = [entry[1] for entry in result_data['series'][0]['values']]

        plt.plot(timestamps, values)
        plt.xlabel('Time')
        plt.ylabel(f'{self.variable}')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pltname = f'{self.variable}_{self.run_start.isoformat()}_{self.run_end.isoformat()}.png'
        plt.savefig(pltname)
        plt.close()

        print(f"{self.variable} from {self.database} plotted and saved as {pltname}")
