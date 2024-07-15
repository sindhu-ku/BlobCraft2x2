from datetime import datetime
from BlobCraft2x2.LRS.LRS_query import LRS_blob_maker
from BlobCraft2x2.Mx2.Mx2_query import Mx2_blob_maker
from BlobCraft2x2.SC.SC_query import SC_blob_maker
from BlobCraft2x2.DB import SQLiteDBManager
from BlobCraft2x2.DataManager import load_config, dump

run=50003

def get_first_start_and_last_end(subrun_dict):
    start_times = [datetime.fromisoformat(times['start_time']) for times in subrun_dict.values()]
    end_times = [datetime.fromisoformat(times['end_time']) for times in subrun_dict.values()]

    first_start_time = min(start_times).isoformat()
    last_end_time = max(end_times).isoformat()

    return first_start_time, last_end_time

def get_subrun_dict():

    lrs_config = load_config("config/LRS_parameters.yaml")
    lrs_sqlite = SQLiteDBManager(run=run, filename=lrs_config.get('filename'))
    lrs_subrun_dict = lrs_sqlite.get_subruns(table='lrs_runs_data', start='start_time_unix', end='end_time_unix', subrun='subrun', condition='morcs_run_nr')

    mx2_config = load_config("config/Mx2_parameters.yaml")
    mx2_sqlite = SQLiteDBManager(run=run, filename=mx2_config.get('filename'))
    mx2_subrun_dict =  mx2_sqlite.get_subruns(table='runsubrun', start='subrunstarttime', end='subrunfinishtime', subrun='runsubrun', condition='runsubrun/10000')

    #Test example
    # lrs_subrun_dict = {0: {'start_time': '2024-07-08T10:46:46-05:00', 'end_time': '2024-07-08T10:47:47-05:00'}, 1: {'start_time': '2024-07-08T10:47:47-05:00', 'end_time': '2024-07-08T10:48:47-05:00'}, 2: {'start_time': '2024-07-08T10:48:47-05:00', 'end_time': '2024-07-08T10:49:47-05:00'}, 3: {'start_time': '2024-07-08T10:49:47-05:00', 'end_time': '2024-07-08T10:50:48-05:00'}, 4: {'start_time': '2024-07-08T10:50:48-05:00', 'end_time': '2024-07-08T10:51:48-05:00'}, 5: {'start_time': '2024-07-08T10:51:48-05:00', 'end_time': '2024-07-08T10:52:49-05:00'}, 6: {'start_time': '2024-07-08T10:52:49-05:00', 'end_time': '2024-07-08T10:53:50-05:00'}, 7: {'start_time': '2024-07-08T10:53:50-05:00', 'end_time': '2024-07-08T10:54:50-05:00'}, 8: {'start_time': '2024-07-08T10:54:50-05:00', 'end_time': '2024-07-08T10:55:51-05:00'}, 9: {'start_time': '2024-07-08T10:55:51-05:00', 'end_time': '2024-07-08T10:56:51-05:00'}, 10: {'start_time': '2024-07-08T10:56:51-05:00', 'end_time': '2024-07-08T10:57:51-05:00'}, 11: {'start_time': '2024-07-08T10:57:51-05:00', 'end_time': '2024-07-08T10:58:52-05:00'}, 12: {'start_time': '2024-07-08T10:58:52-05:00', 'end_time': '2024-07-08T10:59:09-05:00'}}
    # mx2_subrun_dict = {1: {'start_time': '2024-07-08T10:47:22-05:00', 'end_time': '2024-07-08T10:49:09-05:00'}, 2: {'start_time': '2024-07-08T10:49:09-05:00', 'end_time': '2024-07-08T10:59:09-05:00'}}

    # Combine and sort all start times
    all_start_times = []
    for subrun_id, times in lrs_subrun_dict.items():
        all_start_times.append((times['start_time'], 'lrs', subrun_id))
    for subrun_id, times in mx2_subrun_dict.items():
        all_start_times.append((times['start_time'], 'mx2', subrun_id))

    all_start_times.sort()

    # Create global subrun dictionary
    global_subrun_dict = {}
    previous_time = None
    global_subrun_id = 0

    for i, (start_time, system, subrun_id) in enumerate(all_start_times):
        if previous_time: global_subrun_dict[global_subrun_id-1]['end_time'] = start_time

        global_subrun_dict[global_subrun_id] = {
            'start_time': start_time,
            'end_time': None,
            'lrs_subrun_id': subrun_id if system == 'lrs' else None,
            'mx2_subrun_id': subrun_id if system == 'mx2' else None
            }

        # Check for overlaps
        if system == 'lrs':
            for mx2_id, mx2_times in mx2_subrun_dict.items():
                if mx2_times['start_time'] <= start_time <= mx2_times['end_time']:
                    global_subrun_dict[global_subrun_id]['mx2_subrun_id'] = mx2_id
        else:
            for lrs_id, lrs_times in lrs_subrun_dict.items():
                if lrs_times['start_time'] <= start_time <= lrs_times['end_time']:
                    global_subrun_dict[global_subrun_id]['lrs_subrun_id'] = lrs_id

        # Increment global subrun ID
        global_subrun_id += 1
        previous_time = start_time

    # Set end time for the last global subrun
    last_global_subrun_id = max(global_subrun_dict.keys())
    last_lrs_end_time = lrs_subrun_dict[max(lrs_subrun_dict.keys())]['end_time']
    last_mx2_end_time = mx2_subrun_dict[max(mx2_subrun_dict.keys())]['end_time']
    global_subrun_dict[last_global_subrun_id]['end_time'] = max(last_lrs_end_time, last_mx2_end_time)

    # Print the global subrun dictionary
    # for global_subrun_id, times in global_subrun_dict.items():
    #     print(f"Global Subrun {global_subrun_id}: Start: {times['start_time']}, End: {times['end_time']}, LRS Subrun: {times['lrs_subrun_id']}, MX2 Subrun: {times['mx2_subrun_id']}")
    # from pprint import pprint
    # pprint(lrs_subrun_dict)
    # pprint(mx2_subrun_dict)

    return global_subrun_dict


def main():
    query_start = datetime.now()

    LRS_summary= LRS_blob_maker(run=run) #get summary LRS info

    Mx2_summary= Mx2_blob_maker(run=run) #get summary Mx2 info

    # #LRS_blob_maker(run=run, dump_all_data=True)   #dumps all tables in LRS DB into a json blob
    # #Mx2_blob_maker(run=run, dump_all_data=True)   #dumps all tables in Mx2 DB into a json blob

    subrun_dict = get_subrun_dict()

    SC_beam_summary = SC_blob_maker(measurement_name="runsdb", run_number=run, subrun_dict=subrun_dict) #get summary SC data for a given subrun_dict

    #SC_blob_maker(measurement_name="ucondb", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subruns into a a json blob

    start, end = get_first_start_and_last_end(subrun_dict)

    #dump summary into sqlite db
    dump(LRS_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='LRS_summary')
    dump(Mx2_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='Mx2_summary')
    dump(SC_beam_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='SC_beam_summary')

    query_end = datetime.now()
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

if __name__ == "__main__":
    main()
