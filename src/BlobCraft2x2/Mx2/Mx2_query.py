#!/usr/bin/env python3

import argparse
from ..DB import SQLiteDBManager
from ..DataManager import dump, load_config, unix_to_iso, clean_subrun_dict
from ..Beam.beam_query import get_beam_summary

def Mx2_blob_maker(run, start=None, end=None, dump_all_data=False):
    print(f"\n----------------------------------------Fetching Mx2 data for the run {run}----------------------------------------")

    config = load_config("config/Mx2_parameters.yaml")
    sqlite = SQLiteDBManager(run=run, filename=config.get('filename'))

    output = {}
    subruns = sqlite.get_subruns(table='runsubrun', start='subrunstarttime', end='subrunfinishtime', subrun='runsubrun', condition='runsubrun/10000')
    if start and end: subruns = clean_subrun_dict(subruns, start=start, end=end)

    start_time = None
    end_time = None
    for subrun, times in subruns.items():
        if start_time is None: start_time = times['start_time']
        end_time = times['end_time']

        columns = sqlite.get_column_names('runsubrun')
        columns = [c.lower() for c in columns]
        conditions = [f'runsubrun/10000={run}', f'runsubrun%10000={subrun}']
        rows = sqlite.query_data(table_name='runsubrun', columns=columns, conditions=conditions)

        data = [dict(zip(columns, row)) for row in rows]

        if not data:
            continue

        info = {key: val for key, val in data[0].items() if key != 'runsubrun'}
        info['subrunstarttime'] = times['start_time']
        info['subrunfinishtime'] = times['end_time']
        info["beam_summary"] = get_beam_summary(times['start_time'], times['end_time'])
        info['run'] = run
        output[subrun] = info

    sqlite.close_connection()

    if not any(output.values()):
        print(f"No data found for run number {run} in the Mx2 database")

    elif dump_all_data:
        dump(output, f'Mx2_all_ucondb_measurements_run-{run}_{start_time}_{end_time}')

    return output

def main():
    parser = argparse.ArgumentParser(description="Query Mx2 SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    args = parser.parse_args()

    run = args.run

    if not run:
        raise ValueError("run number is a required argument")

    Mx2_blob_maker(run, dump_all_data=True)


if __name__ == "__main__":
    main()
