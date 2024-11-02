import argparse
import yaml
from ..DB import SQLiteDBManager
from ..DataManager import dump, unix_to_iso, clean_subrun_dict
from ..Beam.beam_query import get_beam_summary
from .. import IFbeam_config, LRS_config

def LRS_blob_maker(run, start=None, end=None, dump_all_data=False):
    print(f"\n----------------------------------------Fetching LRS data for the run {run}----------------------------------------")
    config = LRS_config
    sqlite = SQLiteDBManager(run=run, filename=config.get('filename'))

    output = {}
    subruns = sqlite.get_subruns(table='lrs_runs_data', start='start_time_unix', end='end_time_unix', subrun='subrun', condition='morcs_run_nr')
    if start and end: subruns = clean_subrun_dict(subruns, start=start, end=end)

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

        # unix_time_columns = {'first_event_tai', 'last_event_tai'}

        for row in data:
            row['start_time'] = times['start_time']
            row['end_time'] = times['end_time']
            # row.update({col: unix_to_iso(row[col]) for col in unix_time_columns if col in row})

        moas_filename = data[0]["active_moas"] #moas filename is stored here which can then be used to get moas info (especially config id)
        if not moas_filename:
            raise ValueError(f"ERROR: No MOAS version found")

        moas_version = moas_filename[5:-4]
        moas_data = sqlite.query_data(table_name='moas_versions', conditions=[f"version=='{moas_version}'"], columns=moas_columns)
        if not moas_data:
            raise ValueError(f"ERROR: No data found for MOAS version extracted from filename: {moas_filename}")
        if len(moas_data) > 1:
            raise ValueError(f"ERROR: Multiple MOAS versions found for this run/subrun {moas_version}")
        moas_dict = [dict(zip(moas_columns, row)) for row in moas_data]

        data[0].update(moas_dict[0])
        if IFbeam_config['enabled']:
            data[0]["beam_summary"] = get_beam_summary(times['start_time'], times['end_time'])
        output[subrun] = data[0]
        output[subrun]['run'] = run

        if not dump_all_data: continue

        moas_channels_columns = config.get('moas_channels', [])
        config_ids = [moas_row['config_id'] for moas_row in moas_dict]
        if len(config_ids) > 1:
            raise ValueError(f"ERROR: Multiple config_id values found for MOAS version {moas_version}")

        moas_channels_data = sqlite.query_data(table_name='moas_channels', conditions=[f"config_id=={config_id}"], columns=moas_channels_columns)
        if not moas_channels_data:
            raise ValueError(f"ERROR: No MOAS channels data found")

        moas_channels_dict = [dict(zip(moas_channels_columns, row)) for row in moas_channels_data] #Then get the channel information based on the config id for that particular run/subrun
        output[subrun]["moas_channels"] = moas_channels_dict

    sqlite.close_connection()

    if not any(output.values()):
        print(f"No data found for run number {run} in the LRS database")

    elif dump_all_data:
        fname = f'LRS_all_ucondb_measurements_run-{run}_{start_time}_{end_time}'
        dump(output, fname)

    return output


def main():
    parser = argparse.ArgumentParser(description="Query LRS SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    args = parser.parse_args()

    run = args.run

    if not run:
        raise ValueError("run number is a required argument")

    LRS_blob_maker(run, dump_all_data=True)


if __name__ == "__main__":
    main()
