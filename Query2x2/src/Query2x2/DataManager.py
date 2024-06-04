#!/usr/bin/env python3

import pandas as pd
import json
from zoneinfo import ZoneInfo

chicago_tz =  ZoneInfo("America/Chicago")


def dump(data, filename):
    if not data:
        return
    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Dumping data to {filename}")

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
            df["time"] = pd.to_datetime(df["time"], unit="ms")
            self.process_dataframe(df, variables, subsample_interval)
        else:
            raise ValueError("Unsupported source format. Can only handle datatypes from influxsb and psql databases")

        return self.formatted_data

    def process_dataframe(self, df, variables, subsample_interval, tags_dict=None):
        df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert(chicago_tz)
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
