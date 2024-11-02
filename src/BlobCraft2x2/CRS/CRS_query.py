#!/usr/bin/env python3

import argparse
import io
from pathlib import Path
import tarfile

import h5py
import numpy as np

from ..Beam.beam_query import get_beam_summary
from ..DataManager import dump, unix_to_iso
from .. import CRS_config


def get_config_data(h5path):
    with h5py.File(h5path) as h5f:
        tar_bytes = np.array(h5f['daq_configs']).data
        fileobj = io.BytesIO(tar_bytes)
    return tarfile.open(fileobj=fileobj)


def CRS_blob_maker(run, sql_format=False):
    print(f"\n----------------------------------------Fetching CRS data for the run {run}----------------------------------------")

    config = CRS_config
    data_dir = config['data_dir']

    if sql_format:
        output = []
    else:
        output = {}

    files = Path(data_dir).rglob(f'binary-{run:07d}-*.h5')

    for i, path in enumerate(sorted(files)):
        print(path)
        subrun = i + 1
        info = {}

        with h5py.File(path) as f:
            start_time = f['meta'].attrs['created']
            end_time = f['meta'].attrs['modified']
            msg_rate = len(f['msgs']) / (end_time - start_time)

            if sql_format:
                info['subrun'] = subrun

            info['run'] = run
            info['start_time_unix'] = start_time
            info['end_time_unix'] = end_time
            info['start_time'] = unix_to_iso(start_time);
            info['end_time'] = unix_to_iso(end_time);
            info['filename'] = path.name
            info['msg_rate'] = msg_rate

        if sql_format:
            output.append(info)
        else:
            info['beam_summary'] = get_beam_summary(info['start_time'],
                                                    info['end_time'])
            output[subrun] = info

    start_subrun, end_subrun = 1, len(output)
    start_str = output[0]['start_time'] if sql_format else output[start_subrun]['start_time']
    end_str = output[-1]['end_time'] if sql_format else output[end_subrun]['end_time']

    fname = f'CRS_all_ucondb_measurements_run-{run}-{start_str}_{end_str}'
    if sql_format:
        fname += '.SQL'

    dump(output, fname)
    return output


def main():
    parser = argparse.ArgumentParser(description="Query SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    parser.add_argument('--sql-format', action='store_true')
    args = parser.parse_args()

    CRS_blob_maker(args.run, sql_format=args.sql_format)


if __name__ == "__main__":
    main()
