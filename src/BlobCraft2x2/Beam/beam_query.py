#!/usr/bin/env python3

import argparse
import pandas as pd
from ..DataManager import load_config, dump
from ..DB import IFBeamManager
from ..DataManager import parse_datetime

config = load_config('config/IFbeam_parameters.yaml')
manager = IFBeamManager(config)

def calculate_total_pot(df_pot):
    if df_pot.empty:
        return 0.0, 0.0, 0.0
    value = df_pot['value'].sum()

    first_time = df_pot.loc[0, 'time']
    last_time = df_pot.loc[len(df_pot) - 1, 'time']

    return value, first_time, last_time

def get_POT(start, end, total=False):
    manager.set_time_range(start=start, end=end)
    df_pot = manager.get_data(config['pot_device_name'], combine_unit=True)
    if not df_pot.empty: df_pot = df_pot[df_pot['value'] > float(config['pot_threshold'])].reset_index(drop=True)
    if total:
        return calculate_total_pot(df_pot)
    else:
        pot_timseries_data = df_pot.to_dict(orient='records')
        dump(pot_timseries_data, f"BeamPOT_{start}_{end}")

def get_beam_summary(start, end, dump_data=False):
    print(f'Getting beam summary: {start} to {end}')
    pot, first_time, last_time = get_POT(start, end, total=True)
    beam_data =  {
                    "Total_POT": pot,
                    "Start": first_time,
                    "End": last_time
                    }

    if dump_data: dump(beam_data, f"BeamTotalPOT_{start}_{end}")
    return beam_data


def main():
    parser = argparse.ArgumentParser(description="Query IFBeam database and dump data to JSON file.")
    parser.add_argument('--start', type=str, help="Start times for the query (comma-separated if multiple, various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--end', type=str, help="End times for the query (comma-separated if multiple, various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--measurement', type=str, help="Supported: total POT or POT (full timeseries)")

    args = parser.parse_args()

    start = parse_datetime(args.start, is_start=True)
    end = parse_datetime(args.end, is_start=False)

    if args.measurement == "Total POT":
        output = get_beam_summary(start.isoformat(), end.isoformat(), dump_data=True)
    elif args.measurement == "POT":
        get_POT(start.isoformat(), end.isoformat())
    else:
        raise ValueError(f"{args.measurement} not supported!")

if __name__ == "__main__":
    main()
