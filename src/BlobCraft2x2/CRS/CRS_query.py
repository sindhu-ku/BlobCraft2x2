#!/usr/bin/env python3

import argparse
import io
import json
from pathlib import Path
import tarfile

import h5py
import numpy as np

from ..Beam.beam_query import get_beam_summary
from ..DataManager import dump, unix_to_iso
from .. import CRS_config, IFbeam_config


def get_daq_config(f: h5py.File):
    stream = io.BytesIO(np.array(f['daq_configs']).data)
    with tarfile.open(fileobj=stream) as tarf:
        rootname = tarf.getmembers()[0].name
        chip_id ='1-1-11'
        path = f'{rootname}/asic_configs/config_{chip_id}.json'
        config = json.load(tarf.extractfile(path))

        keys = ['threshold_global',
                'ref_current_trim',
                'vref_dac',
                'vcm_dac',
                'adc_hold_delay',
                'enable_periodic_reset',
                'enable_rolling_periodic_reset',
                'periodic_reset_cycles']

        return {k: config[k] for k in keys}

def CRS_blob_maker(run):
    print(f"\n----------------------------------------Fetching CRS data for the run {run}----------------------------------------")

    config = CRS_config
    data_dir = config['data_dir']

    output = {}                 # for CRS_summary
    output_sql = []             # for All_global_subruns

    files = Path(data_dir).rglob(f'binary-{run:07d}-*.h5')

    for i, path in enumerate(sorted(files)):
        print(path)
        subrun = i + 1
        info = {}

        with h5py.File(path) as f:
            start_time = f['meta'].attrs['created']
            end_time = f['meta'].attrs['modified']

            info['run'] = run
            info['start_time_unix'] = start_time
            info['end_time_unix'] = end_time
            info['start_time'] = unix_to_iso(start_time);
            info['end_time'] = unix_to_iso(end_time);
            info['filename'] = path.name

            # SQL format is just used for generating the global subrun table
            # so we keep it simple
            output_sql.append({'subrun': subrun, **info})

            msg_rate = len(f['msgs']) / (end_time - start_time)
            info['msg_rate'] = msg_rate

            info.update(get_daq_config(f))

            if IFbeam_config['enabled']:
                info['beam_summary'] = get_beam_summary(info['start_time'],
                                                        info['end_time'])

            output[subrun] = info

    start_subrun, end_subrun = 1, len(output)
    start_str = output[start_subrun]['start_time']
    end_str = output[end_subrun]['end_time']

    fname = f'CRS_all_ucondb_measurements_run-{run}-{start_str}_{end_str}'

    dump(output, fname)
    dump(output_sql, fname+'.SQL')
    return output


def main():
    parser = argparse.ArgumentParser(description="Query SQLite database and dump data to JSON file.")
    parser.add_argument("--run", type=int, required=True, help="Run number")
    args = parser.parse_args()

    CRS_blob_maker(args.run)


if __name__ == "__main__":
    main()

