#!/usr/bin/env bash

set -o errexit
set -o pipefail

runno=$1; shift

# for All_global_subruns:
CRS_query --run $runno
# for CRS_summary:
CRS_query --sql-format --run $runno

rm -f config/crs_runs.db
scripts/json2sqlite.py -i ./CRS_*.SQL.json -o config/crs_runs.db
rm ./CRS_*.SQL.json

mkdir -p blobs_CRS
rm -f blobs_CRS/CRS_*.json
mv CRS_*.json blobs_CRS

start=$(sqlite3 config/morcs.sqlite "select start_time from run_data where id=$runno" | tr ' ' 'T' | cut -c 1-16)
# lol
start_zone_offset=$(scripts/get_zone_offset.py "$start")
start=$start$start_zone_offset

end=$(sqlite3 config/morcs.sqlite "select end_time from run_data where id=$runno" | tr ' ' 'T' | cut -c 1-16)
end_zone_offset=$(scripts/get_zone_offset.py "$end")
end=$end$end_zone_offset

mkdir -p output
rm -f "output/runs_$runno.db"

scripts/test_runsdb.py -o "output/runs_$runno" --run "$runno" --start "$start" --end "$end"
