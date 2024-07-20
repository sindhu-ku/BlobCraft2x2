#!/usr/bin/env bash

outname=mx2x2runs_v0.1_alpha1

# on daq02:
cd /data/software/BlobCraft2x2
rm CRS_*.json
for run in 50005 50011 50017 50018; do
    CRS_query --run $run
    CRS_query --run $run --sql-format
done

# back at the ranch:

rm -rf blobs_CRS blobs_tmp
mkdir -p blobs_CRS blobs_tmp
scp 'acd-daq02:/data/software/BlobCraft2x2/CRS_*.json' blobs_CRS
mv blobs_CRS/*.SQL.json blobs_tmp

# HACK HACK HACK
rm -f config/crs_runs.db
for f in blobs_tmp/*.SQL.json; do
    scripts/json2sqlite.py -i "$f" -o config/crs_runs.db
done

rm -f "$outname"

scripts/test_runsdb.py -o "$outname" --run 50005 --start 2024-07-02T16:31:47 --end 2024-07-04T12:47:06
# scripts/test_runsdb.py -o "$outname" --run 50011 --start 2024-07-07T11:19:35 --end 2024-07-08T09:45:57
scripts/test_runsdb.py -o "$outname" --run 50017 --start 2024-07-08T13:43:45 --end 2024-07-10T09:21:59
scripts/test_runsdb.py -o "$outname" --run 50018 --start 2024-07-10T09:36:33 --end 2024-07-12T04:03:32
