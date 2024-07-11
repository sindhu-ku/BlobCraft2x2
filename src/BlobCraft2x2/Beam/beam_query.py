#!/usr/bin/env python3

import argparse
from ..DataManager import load_config, dump
from ..DB import IFBeamManager
from ..DataManager import parse_datetime

config = load_config('config/IFbeam_parameters.yaml')
manager = IFBeamManager(config)

def get_beam_summary(start, end, dump_data=False):
    manager.set_time_range(start=start, end=end)
    pot, first_time, last_time = manager.get_total_pot(config['pot_device_name'])
    beam_data = {"Beam_summary":
                    {
                    "Total_POT": pot,
                    "Start": first_time,
                    "End": last_time
                    }
                }
    if dump_data: dump(beam_data, f"BeamTotalPOT_{start}_{end}")
    return beam_data

def get_timeseries(start, end):
    manager.set_time_range(start=start, end=end)
    pot_timseries_data = manager.get_pot(config['pot_device_name'])
    dump(pot_timseries_data, f"BeamPOT_{start}_{end}")

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
        get_timeseries(start.isoformat(), end.isoformat())
    else:
        raise ValueError(f"{args.measurement} not supported!")

if __name__ == "__main__":
    main()
