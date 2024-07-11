from datetime import datetime
from BlobCraft2x2.LRS.LRS_query import LRS_blob_maker
from BlobCraft2x2.SC.SC_query import SC_blob_maker
from BlobCraft2x2.DB import SQLiteDBManager
from BlobCraft2x2.DataManager import load_config, dump

run=20

def get_first_start_and_last_end(subrun_dict):
    start_times = [datetime.fromisoformat(times['start_time']) for times in subrun_dict.values()]
    end_times = [datetime.fromisoformat(times['end_time']) for times in subrun_dict.values()]

    first_start_time = min(start_times).isoformat()
    last_end_time = max(end_times).isoformat()

    return first_start_time, last_end_time

def get_subrun_dict():

    lrs_config = load_config("config/LRS_parameters.yaml")
    lrs_sqlite = SQLiteDBManager(run=run, filename=lrs_config.get('filename'))
    lrs_subrun_dict = lrs_sqlite.get_subruns()
    return lrs_subrun_dict


def main():
    query_start = datetime.now()

    LRS_summary= LRS_blob_maker(run=run) #get summary LRS info

    LRS_blob_maker(run=run, dump_all_data=True)   #dumps all tables in LRS DB into a json blob

    subrun_dict = get_subrun_dict()

    SC_beam_summary = SC_blob_maker(measurement_name="runsdb", run_number=run, subrun_dict=subrun_dict) #get summary SC data for a given subrun_dict

    SC_blob_maker(measurement_name="ucondb", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subruns into a a json blob

    start, end = get_first_start_and_last_end(subrun_dict)

    #dump summary into sqlite db
    dump(LRS_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='LRS_summary')
    dump(SC_beam_summary, filename=f'Runsdb_run_{run}_{start}_{end}', format='sqlite', tablename='SC_beam_summary')

    query_end = datetime.now()
    print("----------------------------------------END OF QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

if __name__ == "__main__":
    main()
