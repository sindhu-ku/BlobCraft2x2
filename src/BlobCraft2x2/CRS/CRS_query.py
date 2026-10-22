#!/usr/bin/env python3

import argparse
import io
import json
from pathlib import Path
import re
import sys
import tarfile

import h5py
import numpy as np

from ..Beam.beam_query import get_beam_summary
from ..DataManager import dump, unix_to_iso
from .. import CRS_config, IFbeam_config


def get_daq_config(f: h5py.File):
    keys = ['threshold_global',
            'ref_current_trim',
            'vref_dac',
            'vcm_dac',
            'adc_hold_delay',
            'enable_periodic_reset',
            'enable_rolling_periodic_reset',
            'periodic_reset_cycles']

    try:
        stream = io.BytesIO(np.array(f['daq_configs']).data)
        with tarfile.open(fileobj=stream) as tarf:
            rex = r'asic_configs/config_[0-9-]+\.json'
            path = next(m.name for m in tarf.getmembers()
                        if re.search(rex, m.name))
            config = json.load(tarf.extractfile(path))
            return {k: config.get(k, -1) for k in keys}
    except:
        fname = Path(f.filename).name
        print(f'Warning: Could not get daq_configs from {fname}',
              file=sys.stderr)
        return {k: -1 for k in keys}


def get_data_stream(path: Path):
    # metacat metadata, declared by morcs run control
    try:
        with open(path.with_name(path.name + '.json')) as f:
            return json.load(f)['metadata'].get('core.data_stream')
    except (OSError, ValueError, KeyError):
        return None


def count_ext_triggers(f: h5py.File, io_group, block=100000):
    # count beam triggers for uptime
    io_groups = f['msg_headers'][:]['io_groups'] 
    idx = np.flatnonzero(io_groups == io_group)
    msgs = f['msgs']
    n = 0
    for start in range(0, len(io_groups), block):
        sel = idx[(idx >= start) & (idx < start + block)] - start
        if not len(sel): continue
        for msg in msgs[start:start + block][sel]:
            msg = msg.tobytes()
            if msg[0] == ord('D'):
                n += msg[8::16].count(ord('T'))
    return n


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

            info['data_stream'] = get_data_stream(path)

            if 'beam_trigger_io_group' in config: # -1: not counted, only physics data is
                info['N_beam_triggers'] = count_ext_triggers(f, config['beam_trigger_io_group']) \
                    if info['data_stream'] == 'physics' else -1

            if IFbeam_config['enabled']:
                info['beam_summary'] = get_beam_summary(info['start_time'],
                                                        info['end_time'])

            output[subrun] = info

    if not output:
        return {}

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

