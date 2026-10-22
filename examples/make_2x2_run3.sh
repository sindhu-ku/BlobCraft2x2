#!/usr/bin/env bash

outname=2x2runs_run3

minrun=70000
maxrun=79999

# TODO: run-3 paths on acdaq5
crs_dir=TODO
packet_dir=TODO
lrs_dir=TODO
lrs_det_config=TODO # lrsdetconfig.db
morcs_db=TODO # morcs.sqlite
mx2_db=TODO # daqSQLiteDB.db

ln -sf configs/2x2_run3 config
cp -f $lrs_det_config config/lrsdetconfig.db
cp -f $morcs_db config/morcs.sqlite
cp -f $mx2_db config/daqSQLiteDB.db

mkdir -p output
scripts/find_runs_to_process.py --minrun $minrun --maxrun $maxrun

scripts/build_file_index.py -o config/files.2x2_run3.crs.pkl \
    --binary2packet --ext h5 \
    --path $crs_dir \
    --packet-dir $packet_dir

scripts/build_file_index.py -o config/files.2x2_run3.lrs.pkl \
    --ext data \
    --path $lrs_dir

scripts/update_spills.py # beam summaries and uptime read the spills from here

cat runs_to_process.txt | parallel -u -j 10 scripts/make_db.sh

rm -f "$outname.sqlite"
scripts/merge_sqlite.py "$outname.sqlite" output/runs_*.db

scripts/update_uptime.py -d "$outname.sqlite"

scripts/add_path_column.py -i config/files.2x2_run3.crs.pkl -d "$outname.sqlite" -t CRS_summary -c nersc_path
scripts/add_path_column.py -i config/files.2x2_run3.lrs.pkl -d "$outname.sqlite" -t LRS_summary -c nersc_path

scripts/sqlite2excel.py -o "$outname.xlsx" -i "$outname.sqlite"
