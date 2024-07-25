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
from BlobCraft2x2.DataManager import parse_datetime

# run=50014
# start="2024-07-08T11:42:18"
# end="2024-07-08T13:35:51"
shift_subrun=10000000
subrun_timediff=5 #seconds

def clean_global_subrun_dict(global_subrun_dict, run): #remove really small subruns
    final_global_subrun_dict = {}
    new_global_subrun_id = run * shift_subrun
    time_shift = timedelta(seconds=0)
    for old_id, times in sorted(global_subrun_dict.items()):
        start_time = datetime.fromisoformat(times['start_time'])
        end_time = datetime.fromisoformat(times['end_time'])
        duration = (end_time - start_time).total_seconds()

        if duration < subrun_timediff:
            time_shift += timedelta(seconds=duration)
            continue

        start_time = start_time - time_shift
        time_shift = timedelta(seconds=0)

        final_global_subrun_dict[new_global_subrun_id] = {
            **times,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration': str(end_time - start_time),
        }
        new_global_subrun_id += 1
    return final_global_subrun_dict

def get_subrun_dict(run, morcs_start, morcs_end):
    def load_subrun_data(config_file, table, start, end, subrun, condition):
        config = load_config(config_file)
        sqlite = SQLiteDBManager(run=run, filename=config.get('filename'))
        data = sqlite.get_subruns(table=table, start=start, end=end, subrun=subrun, condition=condition)
        return clean_subrun_dict(data, start=morcs_start, end=morcs_end)

    crs_subrun_dict = load_subrun_data("config/CRS_parameters.yaml", 'crs_runs_data', 'start_time_unix', 'end_time_unix', 'subrun', 'morcs_run_nr')
    lrs_subrun_dict = load_subrun_data("config/LRS_parameters.yaml", 'lrs_runs_data', 'start_time_unix', 'end_time_unix', 'subrun', 'morcs_run_nr')
    mx2_subrun_dict = load_subrun_data("config/Mx2_parameters.yaml", 'runsubrun', 'subrunstarttime', 'subrunfinishtime', 'runsubrun', 'runsubrun/10000')
    #Test example
    # lrs_subrun_dict = {0: {'start_time': '2024-07-08T10:46:46-05:00', 'end_time': '2024-07-08T10:47:47-05:00'}, 1: {'start_time': '2024-07-08T10:47:47-05:00', 'end_time': '2024-07-08T10:48:47-05:00'}, 2: {'start_time': '2024-07-08T10:48:47-05:00', 'end_time': '2024-07-08T10:49:47-05:00'}, 3: {'start_time': '2024-07-08T10:49:47-05:00', 'end_time': '2024-07-08T10:50:48-05:00'}, 4: {'start_time': '2024-07-08T10:50:48-05:00', 'end_time': '2024-07-08T10:51:48-05:00'}, 5: {'start_time': '202407-08T10:51:48-05:00', 'end_time': '2024-07-08T10:52:49-05:00'}, 6: {'start_time': '2024-07-08T10:52:49-05:00', 'end_time': '2024-07-08T10:53:50-05:00'}, 7: {'start_time': '2024-07-08T10:53:50-05:00', 'end_time': '2024-07-08T10:54:50-05:00'}, 8: {'start_time': '2024-07-08T10:54:50-05:00', 'end_time': '2024-07-08T10:55:51-05:00'}, 9: {'start_time': '2024-07-08T10:55:51-05:00', 'end_time': '2024-07-08T10:56:51-05:00'}, 10: {'start_time': '2024-07-08T10:56:51-05:00', 'end_time': '2024-0708T10:57:51-05:00'}, 11: {'start_time': '2024-07-08T10:57:51-05:00', 'end_time': '2024-07-08T10:58:52-05:00'}, 12: {'start_time': '2024-07-08T10:58:52-05:00', 'end_time': '2024-07-08T10:59:09-05:00'}}
    # mx2_subrun_dict = {1: {'start_time': '2024-07-08T10:47:22-05:00', 'end_time': '2024-07-08T10:49:09-05:00'}, 2: {'start_time': '2024-07-08T10:49:09-05:00', 'end_time': '2024-07-08T10:59:15-05:00'}}

    # HACK
    # TODO: Key subrun dict on (run, subrun) instead of subrun
    if run == 50005:
        run = 50006
        mx2_subrun_dict2 = load_subrun_data("config/Mx2_parameters.yaml", 'runsubrun', 'subrunstarttime', 'subrunfinishtime', 'runsubrun', 'runsubrun/10000')
        run = 50005

    subrun_info = []
    for subrun, times in crs_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'crs', 'start', times['run'], subrun),
                            (times['end_time'], 'crs', 'end', times['run'], subrun)])
    for subrun, times in lrs_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'lrs', 'start', times['run'], subrun),
                            (times['end_time'], 'lrs', 'end', times['run'], subrun)])
    for subrun, times in mx2_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'mx2', 'start', times['run'], subrun),
                            (times['end_time'], 'mx2', 'end', times['run'], subrun)])
    # HACK
    if run == 50005:
        for subrun, times in mx2_subrun_dict2.items():
            subrun_info.extend([(times['start_time'], 'mx2', 'start', times['run'], subrun),
                                (times['end_time'], 'mx2', 'end', times['run'], subrun)])

    subrun_info.sort()  # Sort subrun_info by time

    global_subrun_dict = {}
    first_global_subrun = run * shift_subrun
    global_subrun = first_global_subrun - 1
    # crs_running, lrs_running, mx2_running = False, False, False
    now_running = {'crs': (None, None),
                   'lrs': (None, None),
                   'mx2': (None, None)}
    # current_start_time = None

    for i, (subrun_time, system, subrun_type, run, subrun) in enumerate(subrun_info):
        global_subrun = first_global_subrun + i
        if subrun_type == 'start':
            now_running[system] = (run, subrun)
        elif subrun_type == 'end':
            # if subrun_time == global_subrun_dict[global_subrun]['end_time']:
            #     global_subrun -= 1
            #     continue
            now_running[system] = (None, None)

        if global_subrun != first_global_subrun:
            global_subrun_dict[global_subrun - 1]['end_time'] = subrun_time

        global_subrun_dict[global_subrun] = {
            'global_run': run,
            'start_time': subrun_time,
            'end_time': None,
            'crs_run': now_running['crs'][0],
            'crs_subrun': now_running['crs'][1],
            'lrs_run': now_running['lrs'][0],
            'lrs_subrun': now_running['lrs'][1],
            'mx2_run': now_running['mx2'][0],
            'mx2_subrun': now_running['mx2'][1],
        }

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
    # HACK
    if args.run == 50005:
        Mx2_summary2 = Mx2_blob_maker(run=50006, start=args.start, end=args.end) #get summary Mx2 info

    #LRS_blob_maker(run=run, start=start, end=end, dump_all_data=True)   #dumps all tables in LRS DB into a json blob
    #Mx2_blob_maker(run=run, start=start, end=end, dump_all_data=True)   #dumps all tables in Mx2 DB into a json blob

    # morcs_start = parse_datetime(args.start, is_start=True)
    # morcs_end = parse_datetime(args.end, is_start=False)
    # subrun_dict = get_subrun_dict(args.run, morcs_start, morcs_end)
    subrun_dict = get_subrun_dict(args.run, args.start, args.end)

    SC_beam_summary = SC_blob_maker(measurement_name="runsdb", run_number=args.run, subrun_dict=subrun_dict) #get summary SC data for a given subrun_dict

    #SC_blob_maker(measurement_name="ucondb", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subrun_info into a a json blob

    filename = args.output
    if not filename:
        filename =  f'Runsdb_run_{args.run}_{args.start}_{args.end}'

    #dump summary into sqlite db
    dump(subrun_dict, filename=filename, format='sqlite-global', tablename='All_global_subruns', is_global_subrun=True)
    dump(SC_beam_summary, filename=filename, format='sqlite', tablename='SC_beam_summary', global_run=args.run, is_global_subrun=True)
    dump(CRS_summary, filename=filename, format='sqlite', tablename='CRS_summary', global_run=args.run)
    dump(LRS_summary, filename=filename, format='sqlite', tablename='LRS_summary', global_run=args.run)
    dump(Mx2_summary, filename=filename, format='sqlite', tablename='Mx2_summary', global_run=args.run)
    # HACK
    if args.run == 50005:
        dump(Mx2_summary2, filename=filename, format='sqlite', tablename='Mx2_summary', global_run=args.run)


    query_end = datetime.now()
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

if __name__ == "__main__":
    main()
