#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import sqlite3

import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-i', '--input', required=True, type=Path)
    ap.add_argument('-o', '--output', required=True, type=Path)
    ap.add_argument('-n', '--name', help='Name of table',
                    default='crs_runs_data')
    args = ap.parse_args()

    with open(args.input, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    conn = sqlite3.connect(args.output)
    df.to_sql(args.name, conn, if_exists='append', index=False)
    conn.close()


if __name__ == '__main__':
    main()
