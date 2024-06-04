import sqlite3
import json
import argparse
import yaml

config ={}
cursor =None

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def query_data(table_name, conditions, columns=None):
    if columns:
        columns_str = ", ".join(columns)
    else:
        columns_str = "*"

    condition_str = " AND ".join(conditions)

    query = f"SELECT {columns_str} FROM {table_name} WHERE {condition_str}"
    cursor.execute(query)

    rows = cursor.fetchall()
    return rows

def dump_to_json(filename, data):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Data written to {filename}")


def get_moas_version_data(moas_filename):
    moas_version = moas_filename[5:-4]
    if not moas_version: raise ValueError(f"MOAS file not found!")

    moas_columns = config.get('moas_versions', [])
    if not moas_columns: raise ValueError("No columns specified for moas_versions in the config file.")

    moas_data = query_data(table_name='moas_versions', conditions=[f"version=='{moas_version}'"], columns=moas_columns)

    if not moas_data: raise ValueError(f"ERROR: No data found for MOAS version extracted from filename: {moas_filename}")
    if len(moas_data) > 1: raise ValueError(f"Multiple MOAS versions found for version {moas_version}")

    moas_dict = [dict(zip(moas_columns, row)) for row in moas_data]

    return moas_dict

def get_moas_channels_data(config_id):

    moas_channels_columns = config.get('moas_channels', [])
    moas_channels_data = query_data(table_name='moas_channels', conditions=[f"config_id=={config_id}"], columns=moas_channels_columns)
    if not moas_channels_data: raise ValueError(f"ERROR: No MOAS channels data found")

    moas_channels_dict = [dict(zip(moas_channels_columns, row)) for row in moas_channels_data]

    return moas_channels_dict

def main(run, subrun):
    global config
    global cursor

    config = load_config("parameters.yaml")
    db_filename = config.get('filename')
    if not db_filename:
        raise ValueError("Database filename not specified in the config file.")
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    output = {}

    meta_columns = config.get('lrs_runs_data', [])
    meta_rows = query_data(table_name='lrs_runs_data', conditions=[f"morcs_run_nr=={run}", f"subrun=={subrun}"], columns=meta_columns)
    data = [dict(zip(meta_columns, row)) for row in meta_rows]

    moas_filename = data[0]["active_moas"]
    if not moas_filename: raise ValueError(f"ERROR: No MOAS version found")

    moas_dict = get_moas_version_data(moas_filename)
    data[0].update(moas_dict[0])
    output["lrs_data"] = data[0]

    config_ids = [moas_row['config_id'] for moas_row in moas_dict]
    if len(config_ids) > 1: raise ValueError(f"Multiple config_id values found for MOAS version {moas_version}")
    config_id = config_ids[0]

    moas_channels_dict= get_moas_channels_data(config_id)
    output["moas_channels"] = moas_channels_dict

    if any(output.values()):
        output_filename = f'LRS_run-{run}_subrun-{subrun}.json'
        dump_to_json(output_filename, output)
    else:
        print(f"No data found for run number {run} and subrun number {subrun} in the LRS database")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    parser.add_argument("--subrun", type=int, required=True, help="Subrun number")
    args = parser.parse_args()
    main(args.run, args.subrun)
