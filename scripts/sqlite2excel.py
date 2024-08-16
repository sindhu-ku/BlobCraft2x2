#!/usr/bin/env python3

import argparse
from pathlib import Path
import sqlite3

import pandas as pd


def sqlite_to_excel(sqlite_file, excel_file):
    conn = sqlite3.connect(sqlite_file)

    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    writer = pd.ExcelWriter(excel_file, engine='openpyxl')

    for table_name in tables:
        table_name = table_name[0]
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        df.to_excel(writer, sheet_name=table_name, index=False)

    writer.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-i', '--input', required=True, type=Path)
    ap.add_argument('-o', '--output', required=True, type=Path)
    args = ap.parse_args()

    sqlite_to_excel(args.input, args.output)


if __name__ == '__main__':
    main()
