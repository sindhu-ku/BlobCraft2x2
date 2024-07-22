#!/usr/bin/env python3

import pandas as pd
import json
import csv
from zoneinfo import ZoneInfo
from dateutil import parser as date_parser
from datetime import datetime, time
import yaml
from .DB import SQLiteDBManager
from collections.abc import Mapping

chicago_tz =  ZoneInfo("America/Chicago")
default_utc_time = "1969-12-31T18:00:00-06:00"

def parse_datetime(date_str, is_start):
    dt = date_parser.parse(date_str)
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=chicago_tz)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        if is_start:
            return datetime.combine(dt.date(), time.min, tzinfo=chicago_tz)
        else:
            return datetime.combine(dt.date(), time.max, tzinfo=chicago_tz)
    return dt.astimezone(chicago_tz)

def unix_to_iso(unix_time):
    return datetime.fromtimestamp(unix_time, tz=chicago_tz).isoformat()

def clean_subrun_dict(subrun_dict, start, end):
    cleaned_subrun_dict = {}
    subrun_info = sorted(subrun_dict.keys())

    for i, subrun in enumerate(subrun_info):
        start_time = subrun_dict[subrun]['start_time']
        end_time = subrun_dict[subrun]['end_time']

        if start_time == default_utc_time:
            if i == 0:
                subrun_dict[subrun]['start_time'] = start
            else:
                prev_subrun = subrun_info[i - 1]
                subrun_dict[subrun]['start_time'] = subrun_dict[prev_subrun]['end_time']
                
        if end_time == default_utc_time:
            if i < len(subrun_info) - 1:
                next_subrun = subrun_info[i + 1]
                subrun_dict[subrun]['end_time'] = subrun_dict[next_subrun]['start_time']
            else:
                subrun_dict[subrun]['end_time'] = end

        cleaned_subrun_dict[subrun] = subrun_dict[subrun]

    return cleaned_subrun_dict

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def dump(data, filename, format='json', tablename='runsdb', global_run=None,
         is_global_subrun=False):
    if not data:
        return
    if format == 'sqlite-global':
        sqlite_manager = SQLiteDBManager(f'{filename}.db', run=-100)
        schema = {
            'global_run': int,
            'start_time': str,
            'end_time': str,
            'crs_run': int,
            'crs_subrun': int,
            'lrs_run': int,
            'lrs_subrun': int,
            'mx2_run': int,
            'mx2_subrun': int
        }
        sqlite_manager.create_table(tablename, schema, is_global_subrun=is_global_subrun)

        global_subrun_data = {
            str(global_subrun): {
                'global_run': info['global_run'],
                'start_time': info['start_time'],
                'end_time': info['end_time'],
                'crs_run': info['crs_run'],
                'crs_subrun': info['crs_subrun'],
                'lrs_run': info['lrs_run'],
                'lrs_subrun': info['lrs_subrun'],
                'mx2_run': info['mx2_run'],
                'mx2_subrun': info['mx2_subrun']
            }
            for global_subrun, info in data.items()
        }

        sqlite_manager.insert_data(tablename, global_subrun_data, global_run=global_run, is_global_subrun=is_global_subrun)
        sqlite_manager.conn.commit()
        sqlite_manager.close_connection()
        print(f"Dumping table {tablename} to sqlite database file {filename}.db")

    elif format=='sqlite':
        sqlite_manager = SQLiteDBManager(f'{filename}.db', run=-100)
        if global_run is not None:
            data = {k: {'global_run': global_run, **v}
                    for k, v in data.items()}
        sqlite_manager.dump_data(data, tablename, global_run=global_run, is_global_subrun=is_global_subrun)
        sqlite_manager.close_connection()
        print(f"Dumping table {tablename} to sqlite database file {filename}.db")
    elif format=='json':
        with open(f'{filename}.json', "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Dumping data to {filename}.json")
    else:
        raise ValueError(f'{format} is an unsupported file format type. It can only be json, and sqlite specifically for runsdb')

class DataManager:
    def __init__(self, data):
        self.data = data
        self.formatted_data = data

    def format(self, source="", variables=[], subsample_interval=None):
        if not self.data:
            print(f"WARNING: No data found")
            return None

        self.formatted_data = []

        if source=="influx":
            for key, data_points in self.data.items():
                measurement_name, tags_dict = key
                df = pd.DataFrame(data_points)
                self.process_dataframe(df, variables, subsample_interval, tags_dict)
        elif source=="psql":
            df = pd.DataFrame(self.data, columns=["time"] + variables)
            df["time"] = pd.to_datetime(df["time"], unit="ms") #because cryostat measurements are in ms
            self.process_dataframe(df, variables, subsample_interval)
        else:
            raise ValueError("Unsupported source format. Can only handle datatypes from influxsb and psql databases")

        return self.formatted_data

    def process_dataframe(self, df, variables, subsample_interval, tags_dict=None):
        def format_time(time_str): #because sometimes isoformat in influxdb can be  of this format 2024-05-28T07:54:56Z and pd.to_datetime complains without the microseconds
            if not isinstance(time_str, str): return time_str
            if '.' in time_str:
                split_time = time_str.split('.')
                microseconds = split_time[1]
                if len(microseconds) < 7:
                    microseconds += '0' * (7 - len(microseconds))
                    return split_time[0] + '.' + microseconds + 'Z'
                else:
                    return time_str
            else:
                return time_str[:-1] + '.000000Z'

        df["time"] = df["time"].apply(format_time)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df["time"] = df["time"].dt.tz_convert(chicago_tz)

        if subsample_interval is not None:
            formatted_entries = self.subsample(df, subsample_interval=subsample_interval)
        else:
            formatted_entries = df.to_dict("records")

        for entry in formatted_entries:
            formatted_entry = {
                "time": entry["time"].isoformat(),
                **{var: entry[var] for var in variables}
            }
            if tags_dict:
                formatted_entry["tags"] = tags_dict

            self.formatted_data.append(formatted_entry)

    def subsample(self, df, subsample_interval):
        df_resampled = df.resample(subsample_interval, on="time").mean().dropna()
        formatted_entries = df_resampled.reset_index().to_dict("records")
        return formatted_entries
