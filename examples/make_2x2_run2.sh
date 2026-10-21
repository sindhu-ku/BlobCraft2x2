#!/usr/bin/env bash

outname=2x2runs_run2

minrun=60000
maxrun=65000

ln -sf configs/2x2_run2 config
cp -f /global/cfs/cdirs/dune/www/data/2x2/LRS_det_config_run2/lrsdetconfig.db config/
cp -f /global/cfs/cdirs/dune/www/data/2x2/DB/morcs/run2/morcs.sqlite config/

mkdir -p output
scripts/find_runs_to_process.py --minrun $minrun --maxrun $maxrun

scripts/build_file_index.py -o config/files.2x2_run2.crs.pkl \
    --binary2packet --ext h5 \
    --path /dvs_ro/cfs/cdirs/dune/www/data/2x2/CRS.run2/ColdOperations/data/2025_Operations_Cold \
    --packet-dir /dvs_ro/cfs/cdirs/dune/www/data/2x2/nearline_run2/packet/ColdOperations/data/2025_Operations_Cold

scripts/build_file_index.py -o config/files.2x2_run2.lrs.pkl \
    --ext data \
    --path /dvs_ro/cfs/cdirs/dune/www/data/2x2/LRS_run2

cat runs_to_process.txt | parallel -u -j 10 scripts/make_db.sh

rm -f "$outname.sqlite"
scripts/merge_sqlite.py "$outname.sqlite" output/runs_*.db

scripts/add_path_column.py -i config/files.2x2_run2.crs.pkl -d "$outname.sqlite" -t CRS_summary -c nersc_path
scripts/add_path_column.py -i config/files.2x2_run2.lrs.pkl -d "$outname.sqlite" -t LRS_summary -c nersc_path

scripts/sqlite2excel.py -o "$outname.xlsx" -i "$outname.sqlite"
