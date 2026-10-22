#!/usr/bin/env python3

# Detector uptime = CRS beam triggers / NuMI beam cycles ($A9, also below pot_threshold), both counted from
# uptime_start in config/IFbeam_parameters.yaml up to the end of the last CRS subrun, or up to now if that is more than 'uptime_max_lag' minutes ago. The spills are counted in the spill store from scripts/update_spills.py.

import argparse
from datetime import datetime, timedelta
import sqlite3

import pandas as pd

from BlobCraft2x2.Beam.beam_query import spill_store
from BlobCraft2x2.DataManager import parse_datetime
from BlobCraft2x2 import IFbeam_config, local_tz

table = 'Detector_uptime'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-d', '--db', required=True, help='Merged runs DB: reads CRS_summary, writes the Detector_uptime table')
    ap.add_argument('-s', '--store', default=spill_store, help='Spill store from scripts/update_spills.py, also keeps the uptime history')
    args = ap.parse_args()

    if not IFbeam_config['enabled'] or not IFbeam_config.get('uptime_start'):
        print('IFbeam disabled or no uptime_start in config/IFbeam_parameters.yaml, not updating the uptime')
        return

    start = parse_datetime(IFbeam_config['uptime_start'], is_start=True)
    now = datetime.now(tz=local_tz)
    with sqlite3.connect(args.db) as db:
        last_end = db.execute('SELECT MAX(end_time_unix) FROM CRS_summary').fetchone()[0]
    max_lag = timedelta(minutes=float(IFbeam_config.get('uptime_max_lag', 10)))
    end = now if last_end is None or now - datetime.fromtimestamp(last_end, tz=local_tz) > max_lag \
        else datetime.fromtimestamp(last_end, tz=local_tz)

    with sqlite3.connect(args.store) as store:
        info = dict(store.execute('SELECT key, value FROM store_info').fetchall())
        if datetime.fromisoformat(info['start']) > start:
            raise SystemExit(f"Spill store starts at {info['start']}, after uptime_start; run scripts/update_spills.py first")
        end = min(end, datetime.fromisoformat(info['until'])) # the store is filled before the runs DB is built
        n_spills, last_clock = store.execute('SELECT COUNT(*), MAX(clock) FROM spills WHERE clock >= ? AND clock <= ?',
                                             (int(start.timestamp() * 1e3), int(end.timestamp() * 1e3))).fetchone()
        try:
            history = pd.read_sql_query(f'SELECT * FROM {table}', store)
        except pd.errors.DatabaseError:
            history = pd.DataFrame()
    # Start over if uptime_start was changed
    if not history.empty and history['uptime_start'].iloc[-1] != start.isoformat():
        history = pd.DataFrame()

    with sqlite3.connect(args.db) as db:
        n_triggers = db.execute('SELECT SUM(N_beam_triggers) FROM CRS_summary WHERE N_beam_triggers >= 0 AND end_time_unix > ? AND start_time_unix < ?', # subruns overlapping the period
                                (start.timestamp(), end.timestamp())).fetchone()[0] or 0

    row = {'update_time': datetime.now(tz=local_tz).isoformat(),
           'uptime_start': start.isoformat(),
           'spills_until': end.isoformat(),
           'last_spill_clock': last_clock, # unix ms, as from IFbeam
           'last_spill_time': datetime.fromtimestamp(last_clock / 1e3, tz=local_tz).isoformat() if last_clock else None,
           'N_spills': n_spills,
           'N_beam_triggers': int(n_triggers),
           'uptime_percent': 100 * n_triggers / n_spills if n_spills else None}
    print(row)
    history = pd.concat([history, pd.DataFrame([row])], ignore_index=True)

    with sqlite3.connect(args.store) as store:
        history.to_sql(table, store, if_exists='replace', index=False)
    with sqlite3.connect(args.db) as db:
        history.to_sql(table, db, if_exists='replace', index=False)


if __name__ == '__main__':
    main()
