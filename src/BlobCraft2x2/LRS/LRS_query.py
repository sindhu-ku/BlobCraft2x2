import argparse
from datetime import datetime
import yaml
from ..DB import SQLiteDBManager
from ..DataManager import dump, load_config

def unix_to_iso(unix_time):
    return datetime.fromtimestamp(unix_time).isoformat()

def LRS_blob_maker(run, dump_all_data=False, get_subrun_dict=False):
    print(f"\n----------------------------------------Fetching LRS data for the run {run}----------------------------------------")
    query_start = datetime.now()
    config = load_config("config/LRS_parameters.yaml")
    sqlite = SQLiteDBManager(run=run, filename=config.get('filename'))

    output = {}
    subruns = sqlite.get_subruns()

    start_time = None
    end_time = None
    for subrun, times in subruns.items():
        if start_time is None: start_time = times['start_time']
        end_time = times['end_time']

        meta_columns=None
        moas_columns=None

        if not dump_all_data:
            meta_columns = config.get('lrs_runs_data_summary', [])
            moas_columns = config.get('moas_versions_summary', [])
        else:
            meta_columns = sqlite.get_column_names('lrs_runs_data')
            moas_columns = sqlite.get_column_names('moas_versions')

        meta_rows = sqlite.query_data(table_name='lrs_runs_data', conditions=[f"morcs_run_nr=={run}", f"subrun=={subrun}"], columns=meta_columns)
        data = [dict(zip(meta_columns, row)) for row in meta_rows]

        if not data:
            continue
        unix_time_columns = {'start_time_unix', 'end_time_unix', 'first_event_tai', 'last_event_tai'}

        for row in data:
            row.update({col: unix_to_iso(row[col]) for col in unix_time_columns if col in row})

        moas_filename = data[0]["active_moas"] #moas filename is stored here which can then be used to get moas info (especially config id)
        if not moas_filename:
            raise ValueError(f"ERROR: No MOAS version found")

        moas_dict = sqlite.get_moas_version_data(moas_filename, moas_columns)
        data[0].update(moas_dict[0])
        output[f"subrun_{subrun}"] = data[0]

        if not dump_all_data: continue

        moas_channels_columns = config.get('moas_channels', [])
        config_ids = [moas_row['config_id'] for moas_row in moas_dict]
        if len(config_ids) > 1:
            raise ValueError(f"ERROR: Multiple config_id values found for MOAS version {moas_version}")

        moas_channels_dict = sqlite.get_moas_channels_data(config_ids[0], moas_channels_columns) #Then get the channel information based on the config id for that particular run/subrun
        output[f"subrun_{subrun}"]["moas_channels"] = moas_channels_dict

    if any(output.values()):
        if dump_all_data: output_filename = f'LRS_all-measurments_run-{run}_{start_time}_{end_time}.json'
        else: output_filename = f'LRS_summary_run-{run}_{start_time}_{end_time}.json'
        dump(output, output_filename)
    else:
        print(f"No data found for run number {run} in the LRS database")

    sqlite.close_connection()

    query_end = datetime.now()
    print("----------------------------------------END OF LRS QUERYING AND BLOB MAKING----------------------------------------")
    print("Total querying and blob making time in s: ", query_end - query_start)

    if get_subrun_dict: return subruns


def main():
    parser = argparse.ArgumentParser(description="Query SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    args = parser.parse_args()

    run = args.run

    if not run:
        raise ValueError("run number is a required argument")

    output = LRS_blob_maker(run)


if __name__ == "__main__":
    main()
