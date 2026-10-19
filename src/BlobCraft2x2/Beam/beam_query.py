#!/usr/bin/env python3

import argparse
import pandas as pd
from ..DataManager import dump
from ..DB import IFBeamManager
from ..DataManager import parse_datetime
from .. import IFbeam_config

config = IFbeam_config
manager = IFBeamManager(config)

def calculate_total_pot(df_pot):
    if df_pot.empty:
        return 0.0, 0.0, 0.0, ""
    value = df_pot['value'].sum()

    first_time = df_pot.loc[0, 'time']
    last_time = df_pot.loc[len(df_pot) - 1, 'time']
    devices = ",".join(d for d in config['pot_device_names'] if d in set(df_pot['device'])) or df_pot.loc[0, 'device']

    return value, first_time, last_time, devices

def get_POT(start, end, total=False, device=None):
    manager.set_time_range(start=start, end=end)
    # One POT reading per spill: the first device with a reading above pot_threshold (as in ND_CAFMaker), or only the given device
    dfs = []
    for priority, dev in enumerate([device] if device else config['pot_device_names']):
        df = manager.get_data(dev, combine_unit=True)
        if not df.empty: dfs.append(df.assign(device=dev, priority=priority))
    df_pot = pd.concat(dfs) if dfs else pd.DataFrame(columns=['time', 'value', 'device', 'priority'])
    df_pot = df_pot[df_pot['value'] > float(config['pot_threshold'])]
    if not df_pot.empty:
        # Readings from different devices closer than spill_match_dt belong to the same spill
        df_pot['time_dt'] = pd.to_datetime(df_pot['time'], utc=True, format='ISO8601') # some times have no fractional seconds
        df_pot = df_pot.sort_values('time_dt')
        spill = (df_pot['time_dt'].diff() > pd.Timedelta(seconds=float(config['spill_match_dt']))).cumsum()
        df_pot = df_pot.assign(spill=spill.values).sort_values(['spill', 'priority']).groupby('spill').head(1)
    df_pot = df_pot[['time', 'value', 'device']].reset_index(drop=True)
    if total:
        return calculate_total_pot(df_pot)
    else:
        pot_timseries_data = df_pot.to_dict(orient='records')
        dump(pot_timseries_data, f"BeamPOT_{device.replace(':', '') + '_' if device else ''}{start}_{end}")

def get_beam_summary(start, end, dump_data=False, device=None):
    print(f'Getting beam summary: {start} to {end}')
    pot, first_time, last_time, devices = get_POT(start, end, total=True, device=device)
    beam_data =  {
                    "Total_POT": pot,
                    "Start": first_time,
                    "End": last_time,
                    "POT_device": devices
                    }

    if dump_data: dump(beam_data, f"BeamTotalPOT_{device.replace(':', '') + '_' if device else ''}{start}_{end}")
    return beam_data


def main():
    parser = argparse.ArgumentParser(description="Query IFBeam database and dump data to JSON file.")
    parser.add_argument('--start', type=str, help="Start times for the query (comma-separated if multiple, various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--end', type=str, help="End times for the query (comma-separated if multiple, various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')")
    parser.add_argument('--measurement', type=str, help="Supported: total POT or POT (full timeseries)")
    parser.add_argument('--device', type=str, default=None, help="Use only this POT device, e.g. 'E:TOR101'. Default: per spill, the first of pot_device_names in config/IFbeam_parameters.yaml with a reading above pot_threshold")

    args = parser.parse_args()

    start = parse_datetime(args.start, is_start=True)
    end = parse_datetime(args.end, is_start=False)

    if args.measurement == "Total POT":
        output = get_beam_summary(start.isoformat(), end.isoformat(), dump_data=True, device=args.device)
    elif args.measurement == "POT":
        get_POT(start.isoformat(), end.isoformat(), device=args.device)
    else:
        raise ValueError(f"{args.measurement} not supported!")

if __name__ == "__main__":
    main()
