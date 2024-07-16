from datetime import datetime
from BlobCraft2x2.LRS.LRS_query import LRS_blob_maker
from BlobCraft2x2.Mx2.Mx2_query import Mx2_blob_maker
from BlobCraft2x2.SC.SC_query import SC_blob_maker
from BlobCraft2x2.DB import SQLiteDBManager
from BlobCraft2x2.DataManager import load_config, dump, clean_subrun_dict

run=50014
start="2024-07-08T11:42:18"
end="2024-07-08T13:35:51"
shift_subrun=10000000

def get_subrun_dict():
    def load_subrun_data(config_file, table, start, end, subrun, condition):
        config = load_config(config_file)
        sqlite = SQLiteDBManager(run=run, filename=config.get('filename'))
        return clean_subrun_dict(sqlite.get_subruns(table=table, start=start, end=end, subrun=subrun, condition=condition), start=start, end=end)

    lrs_subrun_dict = load_subrun_data("config/LRS_parameters.yaml", 'lrs_runs_data', 'start_time_unix', 'end_time_unix', 'subrun', 'morcs_run_nr')
    mx2_subrun_dict = load_subrun_data("config/Mx2_parameters.yaml", 'runsubrun', 'subrunstarttime', 'subrunfinishtime', 'runsubrun', 'runsubrun/10000')
    #Test example
    # lrs_subrun_dict = {0: {'start_time': '2024-07-08T10:46:46-05:00', 'end_time': '2024-07-08T10:47:47-05:00'}, 1: {'start_time': '2024-07-08T10:47:47-05:00', 'end_time': '2024-07-08T10:48:47-05:00'}, 2: {'start_time': '2024-07-08T10:48:47-05:00', 'end_time': '2024-07-08T10:49:47-05:00'}, 3: {'start_time': '2024-07-08T10:49:47-05:00', 'end_time': '2024-07-08T10:50:48-05:00'}, 4: {'start_time': '2024-07-08T10:50:48-05:00', 'end_time': '2024-07-08T10:51:48-05:00'}, 5: {'start_time': '202407-08T10:51:48-05:00', 'end_time': '2024-07-08T10:52:49-05:00'}, 6: {'start_time': '2024-07-08T10:52:49-05:00', 'end_time': '2024-07-08T10:53:50-05:00'}, 7: {'start_time': '2024-07-08T10:53:50-05:00', 'end_time': '2024-07-08T10:54:50-05:00'}, 8: {'start_time': '2024-07-08T10:54:50-05:00', 'end_time': '2024-07-08T10:55:51-05:00'}, 9: {'start_time': '2024-07-08T10:55:51-05:00', 'end_time': '2024-07-08T10:56:51-05:00'}, 10: {'start_time': '2024-07-08T10:56:51-05:00', 'end_time': '2024-0708T10:57:51-05:00'}, 11: {'start_time': '2024-07-08T10:57:51-05:00', 'end_time': '2024-07-08T10:58:52-05:00'}, 12: {'start_time': '2024-07-08T10:58:52-05:00', 'end_time': '2024-07-08T10:59:09-05:00'}}
    # mx2_subrun_dict = {1: {'start_time': '2024-07-08T10:47:22-05:00', 'end_time': '2024-07-08T10:49:09-05:00'}, 2: {'start_time': '2024-07-08T10:49:09-05:00', 'end_time': '2024-07-08T10:59:15-05:00'}}

    subrun_info = []
    for subrun, times in lrs_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'lrs', 'start', subrun), (times['end_time'], 'lrs', 'end', subrun)])
    for subrun, times in mx2_subrun_dict.items():
        subrun_info.extend([(times['start_time'], 'mx2', 'start', subrun), (times['end_time'], 'mx2', 'end', subrun)])

    subrun_info.sort()  # Sort subrun_info by time

    global_subrun_dict = {}
    global_subrun = run * shift_subrun
    lrs_running, mx2_running = False, False
    current_start_time = None

    def update_global_subrun_dict(start_time, lrs_subrun=None, mx2_subrun=None):
        global_subrun_dict[global_subrun] = {
            'run': run,
            'start_time': start_time,
            'end_time': None,
            'lrs_subrun': lrs_subrun,
            'mx2_subrun': mx2_subrun
        }

    for subrun_time, system, subrun_type, subrun in subrun_info:
        if subrun_type == 'start':
            if not lrs_running and not mx2_running:
                current_start_time = subrun_time
                update_global_subrun_dict(current_start_time, subrun if system == 'lrs' else None, subrun if system == 'mx2' else None)
            elif system == 'lrs' and not lrs_running:
                global_subrun_dict[global_subrun]['end_time'] = subrun_time
                global_subrun += 1
                current_start_time = subrun_time
                update_global_subrun_dict(current_start_time, subrun, global_subrun_dict[global_subrun - 1]['mx2_subrun'])
            elif system == 'mx2' and not mx2_running:
                global_subrun_dict[global_subrun]['end_time'] = subrun_time
                global_subrun += 1
                current_start_time = subrun_time
                update_global_subrun_dict(current_start_time, global_subrun_dict[global_subrun - 1]['lrs_subrun'], subrun)

            if system == 'lrs':
                lrs_running = True
            elif system == 'mx2':
                mx2_running = True

        elif subrun_type == 'end':
            if subrun_time == global_subrun_dict[global_subrun]['end_time']:
                continue
            if system == 'lrs':
                lrs_running = False
            elif system == 'mx2':
                mx2_running = False

            if not lrs_running and not mx2_running:
                if current_start_time != subrun_time:
                    global_subrun_dict[global_subrun]['end_time'] = subrun_time
                    global_subrun += 1
            elif lrs_running and not mx2_running:
                if current_start_time != subrun_time:
                    global_subrun_dict[global_subrun]['end_time'] = subrun_time
                    global_subrun += 1
                    current_start_time = subrun_time
                    update_global_subrun_dict(current_start_time, global_subrun_dict[global_subrun - 1]['lrs_subrun'], None)
            elif mx2_running and not lrs_running:
                if current_start_time != subrun_time:
                    global_subrun_dict[global_subrun]['end_time'] = subrun_time
                    global_subrun += 1
                    current_start_time = subrun_time
                    update_global_subrun_dict(current_start_time, None, global_subrun_dict[global_subrun - 1]['mx2_subrun'])

    if global_subrun in global_subrun_dict and global_subrun_dict[global_subrun]['end_time'] is None:
        last_lrs_end_time = max(lrs_subrun_dict[subrun]['end_time'] for subrun in lrs_subrun_dict)
        last_mx2_end_time = max(mx2_subrun_dict[subrun]['end_time'] for subrun in mx2_subrun_dict)
        global_subrun_dict[global_subrun]['end_time'] = max(last_lrs_end_time, last_mx2_end_time)

    filtered_global_subrun_dict = {k: v for k, v in global_subrun_dict.items() if v['start_time'] != v['end_time']}

    final_global_subrun_dict = {}
    new_global_subrun_id = run * shift_subrun
    for old_id, times in sorted(filtered_global_subrun_dict.items()):
        final_global_subrun_dict[new_global_subrun_id] = times
        new_global_subrun_id += 1


    #printing
    # for global_subrun, times in final_global_subrun_dict.items():
    #     print(f"Global Subrun {global_subrun}: Start: {times['start_time']}, End: {times['end_time']}, LRS Subrun: {times['lrs_subrun']}, MX2 Subrun: {times['mx2_subrun']}")
    # from pprint import pprint
    # pprint(lrs_subrun_dict)
    # pprint(mx2_subrun_dict)

    return final_global_subrun_dict


def main():
    query_start = datetime.now()

    LRS_summary= LRS_blob_maker(run=run, start=start, end=end) #get summary LRS info

    Mx2_summary= Mx2_blob_maker(run=run, start=start, end=end) #get summary Mx2 info

    #LRS_blob_maker(run=run, start=start, end=end, dump_all_data=True)   #dumps all tables in LRS DB into a json blob
    #Mx2_blob_maker(run=run, start=start, end=end, dump_all_data=True)   #dumps all tables in Mx2 DB into a json blob

    subrun_dict = get_subrun_dict()


    SC_beam_summary = SC_blob_maker(measurement_name="runsdb", run_number=run, subrun_dict=subrun_dict) #get summary SC data for a given subrun_dict

    #SC_blob_maker(measurement_name="ucondb", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subrun_info into a a json blob

    #dump summary into sqlite db
    dump(subrun_dict, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite-global', tablename='Global_subrun_info')
    dump(SC_beam_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='SC_beam_summary')
    dump(LRS_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='LRS_summary')
    dump(Mx2_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='Mx2_summary')


    query_end = datetime.now()
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

if __name__ == "__main__":
    main()
