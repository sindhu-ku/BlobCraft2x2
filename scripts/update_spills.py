#!/usr/bin/env python3

# Keeps one reading per NuMI beam cycle ($A9), also below pot_threshold, from uptime_start in config/IFbeam_parameters.yaml up to now in a local store, so the beam summaries and the detector uptime don't each query IFbeam. Run before the runs DB is built

import argparse
import os
from datetime import datetime
import sqlite3

from BlobCraft2x2.Beam.beam_query import query_spills, spill_store
from BlobCraft2x2.DataManager import parse_datetime
from BlobCraft2x2 import IFbeam_config, local_tz


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-s', '--store', default=spill_store, help='Spill store (sqlite)')
    args = ap.parse_args()

    if not IFbeam_config['enabled'] or not IFbeam_config.get('uptime_start'):
        print('IFbeam disabled or no uptime_start in config/IFbeam_parameters.yaml, not updating the spill store')
        return

    start = parse_datetime(IFbeam_config['uptime_start'], is_start=True)
    end = datetime.now(tz=local_tz)
    match_ms = float(IFbeam_config['spill_match_dt']) * 1e3

    os.makedirs(os.path.dirname(args.store) or '.', exist_ok=True)
    with sqlite3.connect(args.store) as db:
        db.execute('CREATE TABLE IF NOT EXISTS spills (clock INTEGER PRIMARY KEY, time TEXT, value REAL, device TEXT)') # clock: unix ms
        db.execute('CREATE TABLE IF NOT EXISTS store_info (key TEXT PRIMARY KEY, value TEXT)')
        info = dict(db.execute('SELECT key, value FROM store_info').fetchall())
        if 'start' in info and datetime.fromisoformat(info['start']) > start: # uptime_start moved earlier: start over
            db.execute('DELETE FROM spills'); db.execute('DELETE FROM store_info'); info = {}
        if 'start' not in info:
            db.execute("INSERT INTO store_info VALUES ('start', ?)", (start.isoformat(),))

        last_clock = db.execute('SELECT MAX(clock) FROM spills').fetchone()[0]
        t = datetime.fromtimestamp(last_clock / 1e3, tz=local_tz) if last_clock else start
        if last_clock is None:
            last_clock = start.timestamp() * 1e3 - match_ms - 1 # so a spill exactly at uptime_start is stored
        if t < end:
            print(f'Querying spills from {t} to {end}')
            spills = query_spills(t.isoformat(), end.isoformat())
            spills['clock'] = spills['clock'].astype(float).astype('int64')
            # Readings closer than spill_match_dt to the last stored spill belong to it
            spills = spills[spills['clock'] > last_clock + match_ms]
            if not spills.empty:
                db.executemany('INSERT INTO spills VALUES (?, ?, ?, ?)',
                               spills[['clock', 'time', 'value', 'device']].itertuples(index=False))
                last_clock = int(spills['clock'].max())
        until = max(end, datetime.fromisoformat(info['until'])) if 'until' in info else end
        db.execute("INSERT OR REPLACE INTO store_info VALUES ('until', ?)", (until.isoformat(),))
        print(f"{db.execute('SELECT COUNT(*) FROM spills').fetchone()[0]} spills stored, last at "
              f"{datetime.fromtimestamp(last_clock / 1e3, tz=local_tz)}")


if __name__ == '__main__':
    main()
