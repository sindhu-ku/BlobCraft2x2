#!/usr/bin/env python3

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import json

from BlobCraft2x2.LRS.LRS_query import LRS_blob_maker
from BlobCraft2x2.Mx2.Mx2_query import Mx2_blob_maker
from BlobCraft2x2.SC.SC_query import SC_blob_maker
from BlobCraft2x2.DB import SQLiteDBManager
from BlobCraft2x2.DataManager import load_config, dump, clean_subrun_dict

# run=50014
# start="2024-07-08T11:42:18"
# end="2024-07-08T13:35:51"
shift_subrun=10000000
subrun_timediff=5 #seconds

def clean_global_subrun_dict(global_subrun_dict, run): #remove really small subruns
    final_global_subrun_dict = {}
    new_global_subrun_id = run * shift_subrun
    time_shift = None
    for old_id, times in sorted(global_subrun_dict.items()):
        start_time = datetime.fromisoformat(times['start_time'])
        end_time = datetime.fromisoformat(times['end_time'])
        duration = (end_time - start_time).total_seconds()
        if duration == 0: continue
        if times['crs_subrun'] is None or times['lrs_subrun'] is None \
           or times['mx2_subrun'] is None:
            if duration < subrun_timediff:
                time_shift = timedelta(seconds=duration)
                continue

        if time_shift is not None:
            start_time = start_time - time_shift
            time_shift = None

        final_global_subrun_dict[new_global_subrun_id] = {
            'run': times['run'],
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'crs_subrun': times['crs_subrun'],
            'lrs_subrun': times['lrs_subrun'],
            'mx2_subrun': times['mx2_subrun']
        }
        new_global_subrun_id += 1
    return final_global_subrun_dict

def get_subrun_dict(run):
    def load_subrun_data(config_file, table, start, end, subrun, condition):
        config = load_config(config_file)
        sqlite = SQLiteDBManager(run=run, filename=config.get('filename'))
        return clean_subrun_dict(sqlite.get_subruns(table=table, start=start, end=end, subrun=subrun, condition=condition), start=start, end=end)

    crs_subrun_dict = load_subrun_data("config/CRS_parameters.yaml", 'crs_runs_data', 'start_time_unix', 'end_time_unix', 'subrun', 'morcs_run_nr')
    lrs_subrun_dict = load_subrun_data("config/LRS_parameters.yaml", 'lrs_runs_data', 'start_time_unix', 'end_time_unix', 'subrun', 'morcs_run_nr')
    mx2_subrun_dict = load_subrun_data("config/Mx2_parameters.yaml", 'runsubrun', 'subrunstarttime', 'subrunfinishtime', 'runsubrun', 'runsubrun/10000')
    #Test example
    # lrs_subrun_dict = {0: {'start_time': '2024-07-08T10:46:46-05:00', 'end_time': '2024-07-08T10:47:47-05:00'}, 1: {'start_time': '2024-07-08T10:47:47-05:00', 'end_time': '2024-07-08T10:48:47-05:00'}, 2: {'start_time': '2024-07-08T10:48:47-05:00', 'end_time': '2024-07-08T10:49:47-05:00'}, 3: {'start_time': '2024-07-08T10:49:47-05:00', 'end_time': '2024-07-08T10:50:48-05:00'}, 4: {'start_time': '2024-07-08T10:50:48-05:00', 'end_time': '2024-07-08T10:51:48-05:00'}, 5: {'start_time': '202407-08T10:51:48-05:00', 'end_time': '2024-07-08T10:52:49-05:00'}, 6: {'start_time': '2024-07-08T10:52:49-05:00', 'end_time': '2024-07-08T10:53:50-05:00'}, 7: {'start_time': '2024-07-08T10:53:50-05:00', 'end_time': '2024-07-08T10:54:50-05:00'}, 8: {'start_time': '2024-07-08T10:54:50-05:00', 'end_time': '2024-07-08T10:55:51-05:00'}, 9: {'start_time': '2024-07-08T10:55:51-05:00', 'end_time': '2024-07-08T10:56:51-05:00'}, 10: {'start_time': '2024-07-08T10:56:51-05:00', 'end_time': '2024-0708T10:57:51-05:00'}, 11: {'start_time': '2024-07-08T10:57:51-05:00', 'end_time': '2024-07-08T10:58:52-05:00'}, 12: {'start_time': '2024-07-08T10:58:52-05:00', 'end_time': '2024-07-08T10:59:09-05:00'}}
    # mx2_subrun_dict = {1: {'start_time': '2024-07-08T10:47:22-05:00', 'end_time': '2024-07-08T10:49:09-05:00'}, 2: {'start_time': '2024-07-08T10:49:09-05:00', 'end_time': '2024-07-08T10:59:15-05:00'}}

    subrun_info = []
    for subrun, times in crs_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'crs', 'start', subrun), (times['end_time'], 'crs', 'end', subrun)])
    for subrun, times in lrs_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'lrs', 'start', subrun), (times['end_time'], 'lrs', 'end', subrun)])
    for subrun, times in mx2_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'mx2', 'start', subrun), (times['end_time'], 'mx2', 'end', subrun)])

    subrun_info.sort()  # Sort subrun_info by time

    global_subrun_dict = {}
    first_global_subrun = run * shift_subrun
    global_subrun = first_global_subrun - 1
    # crs_running, lrs_running, mx2_running = False, False, False
    now_running = {'crs': None,
                   'lrs': None,
                   'mx2': None}
    # current_start_time = None

    def update_global_subrun_dict(start_time, crs_subrun=None, lrs_subrun=None, mx2_subrun=None):
        global_subrun_dict[global_subrun] = {
            'run': run,
            'start_time': start_time,
            'end_time': None,
            'crs_subrun': crs_subrun,
            'lrs_subrun': lrs_subrun,
            'mx2_subrun': mx2_subrun
        }

    for i, (subrun_time, system, subrun_type, subrun) in enumerate(subrun_info):
        global_subrun = first_global_subrun + i
        if subrun_type == 'start':
            now_running[system] = subrun
        elif subrun_type == 'end':
            # if subrun_time == global_subrun_dict[global_subrun]['end_time']:
            #     global_subrun -= 1
            #     continue
            now_running[system] = None

        if global_subrun != first_global_subrun:
            global_subrun_dict[global_subrun - 1]['end_time'] = subrun_time

        update_global_subrun_dict(subrun_time,
                                  now_running['crs'],
                                  now_running['lrs'],
                                  now_running['mx2'])

    if global_subrun in global_subrun_dict and global_subrun_dict[global_subrun]['end_time'] is None:
        last_crs_end_time = max(crs_subrun_dict[subrun]['end_time'] for subrun in crs_subrun_dict)
        last_lrs_end_time = max(lrs_subrun_dict[subrun]['end_time'] for subrun in lrs_subrun_dict)
        last_mx2_end_time = max(mx2_subrun_dict[subrun]['end_time'] for subrun in mx2_subrun_dict)
        global_subrun_dict[global_subrun]['end_time'] = max(last_crs_end_time,
                                                            last_lrs_end_time,
                                                            last_mx2_end_time)

    final_global_subrun_dict = clean_global_subrun_dict(global_subrun_dict, run)

    #printing
    # for global_subrun, times in final_global_subrun_dict.items():
    #     print(f"Global Subrun {global_subrun}: Start: {times['start_time']}, End: {times['end_time']}, LRS Subrun: {times['lrs_subrun']}, MX2 Subrun: {times['mx2_subrun']}")
    # from pprint import pprint
    # pprint(lrs_subrun_dict)
    # pprint(mx2_subrun_dict)

    return final_global_subrun_dict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run', type=int, default=50014)
    ap.add_argument('--start', default="2024-07-08T11:42:18")
    ap.add_argument('--end', default="2024-07-08T13:35:51")
    ap.add_argument('-o', '--output')
    args = ap.parse_args()

    query_start = datetime.now()

    # HACK
    pattern = f'CRS_all_ucondb_measurements_run-{args.run:05d}*.json'
    path = next(Path('blobs_CRS').glob(pattern))
    with open(path) as f:
        CRS_summary = json.load(f)

    # CRS_summary= CRS_blob_maker(run=run, start=start, end=end) #get summary LRS info
    LRS_summary= LRS_blob_maker(run=args.run, start=args.start, end=args.end) #get summary LRS info

    Mx2_summary= Mx2_blob_maker(run=args.run, start=args.start, end=args.end) #get summary Mx2 info

    #LRS_blob_maker(run=run, start=start, end=end, dump_all_data=True)   #dumps all tables in LRS DB into a json blob
    #Mx2_blob_maker(run=run, start=start, end=end, dump_all_data=True)   #dumps all tables in Mx2 DB into a json blob

    subrun_dict = get_subrun_dict(args.run)

    SC_beam_summary = SC_blob_maker(measurement_name="runsdb", run_number=args.run, subrun_dict=subrun_dict) #get summary SC data for a given subrun_dict

    #SC_blob_maker(measurement_name="ucondb", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subrun_info into a a json blob

    filename = args.output
    if not filename:
        filename =  f'Runsdb_run_{args.run}_{args.start}_{args.end}'

    #dump summary into sqlite db
    dump(subrun_dict, filename=filename, format='sqlite-global', tablename='Global_subrun_info')
    dump(SC_beam_summary, filename=filename, format='sqlite', tablename='SC_beam_summary')
    dump(CRS_summary, filename=filename, format='sqlite', tablename='CRS_summary', run=args.run)
    dump(LRS_summary, filename=filename, format='sqlite', tablename='LRS_summary', run=args.run)
    dump(Mx2_summary, filename=filename, format='sqlite', tablename='Mx2_summary', run=args.run)


    query_end = datetime.now()
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

if __name__ == "__main__":
    main()
