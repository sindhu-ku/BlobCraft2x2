import argparse
from datetime import datetime
import yaml
from ..DB import SQLiteManager
from ..DataManager import dump, load_config

def LRS_blob_maker(run, get_subrun_dict=False):
    query_start = datetime.now()
    config = load_config("config/LRS_parameters.yaml")
    print(f"\n----------------------------------------Fetching LRS data for the run {run}----------------------------------------")
    sqlite = SQLiteManager(config=config, run=run)

    output = {}
    subruns = sqlite.get_subruns()

    for subrun, times in subruns.items():
        meta_columns = sqlite.config.get('lrs_runs_data', [])
        meta_rows = sqlite.query_data(table_name='lrs_runs_data', conditions=[f"morcs_run_nr=={run}", f"subrun=={subrun}"], columns=meta_columns)
        data = [dict(zip(meta_columns, row)) for row in meta_rows]

        if not data:
            continue

        moas_filename = data[0]["active_moas"]
        if not moas_filename:
            raise ValueError(f"ERROR: No MOAS version found")

        moas_dict = sqlite.get_moas_version_data(moas_filename)
        data[0].update(moas_dict[0])
        output[f"subrun_{subrun}"] = data[0]

        config_ids = [moas_row['config_id'] for moas_row in moas_dict]
        if len(config_ids) > 1:
            raise ValueError(f"ERROR: Multiple config_id values found for MOAS version {moas_version}")
        config_id = config_ids[0]

        moas_channels_dict = sqlite.get_moas_channels_data(config_id)
        output[f"subrun_{subrun}"]["moas_channels"] = moas_channels_dict

    if any(output.values()):
        output_filename = f'LRS_run-{run}.json'
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
