#!/usr/bin/env python3

import argparse

from ..DB import SQLiteDBManager
from ..DataManager import dump, load_config
from ..LRS.LRS_query import unix_to_iso


def Mx2_blob_maker(run):
    print(f"\n----------------------------------------Fetching Mx2 data for the run {run}----------------------------------------")

    config = load_config("config/Mx2_parameters.yaml")
    sqlite = SQLiteDBManager(run=None, filename=config.get('filename'))

    columns = sqlite.get_column_names('runsubrun')
    columns = [c.lower() for c in columns]
    condition = f'runsubrun/10000 = {run}'
    rows = sqlite.query_data(table_name='runsubrun', columns=columns,
                             conditions=[condition])
    data = [dict(zip(columns, row)) for row in rows]

    output = {}
    start_times, end_times = set(), set()

    for subrun_dict in data:
        subrun = subrun_dict['runsubrun'] % 10000
        info = {key: val for key, val in subrun_dict.items()
                if key != 'runsubrun'}

        start_times.add(info['subrunstarttime'])
        end_times.add(info['subrunfinishtime'])

        info['subrunstarttime'] = unix_to_iso(info['subrunstarttime'])
        info['subrunfinishtime'] = unix_to_iso(info['subrunfinishtime'])

        output[subrun] = info

    start_str = unix_to_iso(min(start_times))
    end_str = unix_to_iso(max(end_times))

    fname = f'Mx2_all_ucondb_measurements_run-{run}_{start_str}_{end_str}'
    dump(output, fname)


def main():
    parser = argparse.ArgumentParser(description="Query SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    args = parser.parse_args()

    Mx2_blob_maker(args.run)


if __name__ == "__main__":
    main()
