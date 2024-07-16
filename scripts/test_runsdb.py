from datetime import datetime
from BlobCraft2x2.LRS.LRS_query import LRS_blob_maker
from BlobCraft2x2.Mx2.Mx2_query import Mx2_blob_maker
from BlobCraft2x2.SC.SC_query import SC_blob_maker
from BlobCraft2x2.DB import SQLiteDBManager
from BlobCraft2x2.DataManager import load_config, dump

run=50014
start="2024-07-08T11:42:18"
end="2024-07-08T13:35:51"
default_utc_time = "1969-12-31T18:00:00-06:00"


def clean_subrun_dict(subrun_dict):
    cleaned_subrun_dict = {}
    subruns = sorted(subrun_dict.keys())

    for i, subrun in enumerate(subruns):
        start_time = subrun_dict[subrun]['start_time']
        end_time = subrun_dict[subrun]['end_time']

        # Remove entry if both start and end times are default UTC time
        if start_time == default_utc_time and end_time == default_utc_time:
            continue

        # Modify end time if it is default UTC time and not the last subrun
        if end_time == default_utc_time and i < len(subruns) - 1:
            next_subrun = subruns[i + 1]
            subrun_dict[subrun]['end_time'] = subrun_dict[next_subrun]['start_time']

        # Modify end time if it is default UTC time and it is the last subrun
        if end_time == default_utc_time and i == len(subruns) - 1:
            subrun_dict[subrun]['end_time'] = end

        cleaned_subrun_dict[subrun] = subrun_dict[subrun]

    return cleaned_subrun_dict



def get_subrun_dict():

    # lrs_config = load_config("config/LRS_parameters.yaml")
    # lrs_sqlite = SQLiteDBManager(run=run, filename=lrs_config.get('filename'))
    # lrs_subrun_dict = clean_subrun_dict(lrs_sqlite.get_subruns(table='lrs_runs_data', start='start_time_unix', end='end_time_unix', subrun='subrun', condition='morcs_run_nr'))
    #
    # mx2_config = load_config("config/Mx2_parameters.yaml")
    # mx2_sqlite = SQLiteDBManager(run=run, filename=mx2_config.get('filename'))
    # mx2_subrun_dict =  clean_subrun_dict(mx2_sqlite.get_subruns(table='runsubrun', start='subrunstarttime', end='subrunfinishtime', subrun='runsubrun', condition='runsubrun/10000'))

    #Test example
    lrs_subrun_dict = {0: {'start_time': '2024-07-08T10:46:46-05:00', 'end_time': '2024-07-08T10:47:47-05:00'}, 1: {'start_time': '2024-07-08T10:47:47-05:00', 'end_time': '2024-07-08T10:48:47-05:00'}, 2: {'start_time': '2024-07-08T10:48:47-05:00', 'end_time': '2024-07-08T10:49:47-05:00'}, 3: {'start_time': '2024-07-08T10:49:47-05:00', 'end_time': '2024-07-08T10:50:48-05:00'}, 4: {'start_time': '2024-07-08T10:50:48-05:00', 'end_time': '2024-07-08T10:51:48-05:00'}, 5: {'start_time': '2024-07-08T10:51:48-05:00', 'end_time': '2024-07-08T10:52:49-05:00'}, 6: {'start_time': '2024-07-08T10:52:49-05:00', 'end_time': '2024-07-08T10:53:50-05:00'}, 7: {'start_time': '2024-07-08T10:53:50-05:00', 'end_time': '2024-07-08T10:54:50-05:00'}, 8: {'start_time': '2024-07-08T10:54:50-05:00', 'end_time': '2024-07-08T10:55:51-05:00'}, 9: {'start_time': '2024-07-08T10:55:51-05:00', 'end_time': '2024-07-08T10:56:51-05:00'}, 10: {'start_time': '2024-07-08T10:56:51-05:00', 'end_time': '2024-07-08T10:57:51-05:00'}, 11: {'start_time': '2024-07-08T10:57:51-05:00', 'end_time': '2024-07-08T10:58:52-05:00'}, 12: {'start_time': '2024-07-08T10:58:52-05:00', 'end_time': '2024-07-08T10:59:09-05:00'}}
    mx2_subrun_dict = {1: {'start_time': '2024-07-08T10:47:22-05:00', 'end_time': '2024-07-08T10:49:09-05:00'}, 2: {'start_time': '2024-07-08T10:49:09-05:00', 'end_time': '2024-07-08T10:59:15-05:00'}}

    events = []

    # Collect events from lrs_subrun_dict
    for subrun, times in lrs_subrun_dict.items():
        events.append((times['start_time'], 'lrs', 'start', subrun))
        events.append((times['end_time'], 'lrs', 'end', subrun))

    # Collect events from mx2_subrun_dict
    for subrun, times in mx2_subrun_dict.items():
        events.append((times['start_time'], 'mx2', 'start', subrun))
        events.append((times['end_time'], 'mx2', 'end', subrun))

    events.sort()  # Sort events by time

    global_subrun_dict = {}
    global_subrun = run*100000  # Initial global subrun ID
    lrs_running = False
    mx2_running = False
    current_start_time = None

    for event_time, system, event_type, subrun in events:

        if event_type == 'start':
            if not global_subrun==run*100000 and event_time == global_subrun_dict[global_subrun]['end_time']: continue
            if not lrs_running and not mx2_running:
                current_start_time = event_time
                global_subrun_dict[global_subrun] = {
                    'start_time': current_start_time,
                    'end_time': None,
                    'lrs_subrun': subrun if system == 'lrs' else None,
                    'mx2_subrun': subrun if system == 'mx2' else None
                }
            elif system == 'lrs' and not lrs_running:
                if mx2_running:
                    global_subrun_dict[global_subrun]['end_time'] = event_time
                    global_subrun == 1
                    current_start_time = event_time
                    global_subrun_dict[global_subrun] = {
                        'start_time': current_start_time,
                        'end_time': None,
                        'lrs_subrun': subrun,
                        'mx2_subrun': global_subrun_dict[global_subrun-1]['mx2_subrun']
                    }
                else:
                    global_subrun_dict[global_subrun]['end_time'] = event_time
                    current_start_time = event_time
                    global_subrun_dict[global_subrun+1] = {
                        'start_time': current_start_time,
                        'end_time': None,
                        'lrs_subrun': subrun,
                        'mx2_subrun': global_subrun_dict[global_subrun]['mx2_subrun']
                    }
            elif system == 'mx2' and not mx2_running:
                if lrs_running:
                    global_subrun_dict[global_subrun]['end_time'] = event_time
                    global_subrun += 1
                    current_start_time = event_time
                    global_subrun_dict[global_subrun] = {
                        'start_time': current_start_time,
                        'end_time': None,
                        'lrs_subrun': global_subrun_dict[global_subrun-1]['lrs_subrun'],
                        'mx2_subrun': subrun
                    }
                else:
                    global_subrun_dict[global_subrun]['end_time'] = event_time
                    current_start_time = event_time
                    global_subrun_dict[global_subrun+1] = {
                        'start_time': current_start_time,
                        'end_time': None,
                        'lrs_subrun': global_subrun_dict[global_subrun]['lrs_subrun'],
                        'mx2_subrun': subrun
                    }

            if system == 'lrs':
                lrs_running = True
            elif system == 'mx2':
                mx2_running = True

        elif event_type == 'end':
            if event_time == global_subrun_dict[global_subrun]['end_time']: continue
            if system == 'lrs':
                lrs_running = False
            elif system == 'mx2':
                mx2_running = False

            if not lrs_running and not mx2_running:
                global_subrun_dict[global_subrun]['end_time'] = event_time
                global_subrun += 1
            elif lrs_running and not mx2_running:
                global_subrun_dict[global_subrun]['end_time'] = event_time
                global_subrun += 1
                current_start_time = event_time
                global_subrun_dict[global_subrun] = {
                    'start_time': current_start_time,
                    'end_time': None,
                    'lrs_subrun': global_subrun_dict[global_subrun-1]['lrs_subrun'],
                    'mx2_subrun': None
                }
            elif mx2_running and not lrs_running:
                global_subrun_dict[global_subrun]['end_time'] = event_time
                global_subrun += 1
                current_start_time = event_time
                global_subrun_dict[global_subrun] = {
                    'start_time': current_start_time,
                    'end_time': None,
                    'lrs_subrun': None,
                    'mx2_subrun': global_subrun_dict[global_subrun-1]['mx2_subrun']
                }

    # Set end time for the last global subrun
    if global_subrun in global_subrun_dict and global_subrun_dict[global_subrun]['end_time'] is None:
        last_lrs_end_time = max(lrs_subrun_dict[subrun]['end_time'] for subrun in lrs_subrun_dict)
        last_mx2_end_time = max(mx2_subrun_dict[subrun]['end_time'] for subrun in mx2_subrun_dict)
        global_subrun_dict[global_subrun]['end_time'] = max(last_lrs_end_time, last_mx2_end_time)

    #printing
    for global_subrun, times in global_subrun_dict.items():
        print(f"Global Subrun {global_subrun}: Start: {times['start_time']}, End: {times['end_time']}, LRS Subrun: {times['lrs_subrun']}, MX2 Subrun: {times['mx2_subrun']}")
    from pprint import pprint
    pprint(lrs_subrun_dict)
    pprint(mx2_subrun_dict)

    return global_subrun_dict


def main():
    query_start = datetime.now()

    LRS_summary= LRS_blob_maker(run=run) #get summary LRS info

    Mx2_summary= Mx2_blob_maker(run=run) #get summary Mx2 info

    # #LRS_blob_maker(run=run, dump_all_data=True)   #dumps all tables in LRS DB into a json blob
    # #Mx2_blob_maker(run=run, dump_all_data=True)   #dumps all tables in Mx2 DB into a json blob

    subrun_dict = get_subrun_dict()


    #SC_beam_summary = SC_blob_maker(measurement_name="runsdb", run_number=run, subrun_dict=subrun_dict) #get summary SC data for a given subrun_dict

    #SC_blob_maker(measurement_name="ucondb", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subruns into a a json blob

    #dump summary into sqlite db
    # dump(subrun_dict, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite-global', tablename='Global_subrun_info')
    # dump(SC_beam_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='SC_beam_summary')
    # dump(LRS_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='LRS_summary')
    # dump(Mx2_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='Mx2_summary')
    #

    query_end = datetime.now()
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

if __name__ == "__main__":
    main()
