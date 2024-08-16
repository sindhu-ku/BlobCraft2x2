#!/usr/bin/env python3

import argparse
import sqlite3


def merge_sqlite(dest_file, source_files):
    dest_conn = sqlite3.connect(dest_file)
    dest_cursor = dest_conn.cursor()

    for isource, source_file in enumerate(source_files):
        source_conn = sqlite3.connect(source_file)
        source_cursor = source_conn.cursor()

        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = source_cursor.fetchall()

        for table in tables:
            table_name = table[0]
            print(table_name)

            if isource == 0:
                source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                create_table_sql = source_cursor.fetchone()[0]
                dest_cursor.execute(create_table_sql)

            source_cursor.execute(f"SELECT * FROM {table_name}")
            rows = source_cursor.fetchall()
            if rows:
                placeholders = ", ".join("?" * len(rows[0]))
                dest_cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)

        source_conn.close()

    dest_conn.commit()
    dest_conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dest_file')
    ap.add_argument('source_files', nargs='+')
    args = ap.parse_args()

    merge_sqlite(args.dest_file, args.source_files)


if __name__ == '__main__':
    main()
