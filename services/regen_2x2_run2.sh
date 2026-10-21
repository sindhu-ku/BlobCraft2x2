#!/usr/bin/env bash

basedir=/global/cfs/cdirs/dune/www/data/2x2/DB/RunsDB

while true; do
    now=$(date -u +%Y_%m_%d_%H_%M_%S_%Z)
    examples/make_2x2_run2.sh
    cp 2x2runs_run2.sqlite $basedir/history/run2/2x2runs_run2.$now.sqlite
    cp 2x2runs_run2.xlsx $basedir/history/run2/2x2runs_run2.$now.xlsx
    ln -sf ../../history/run2/2x2runs_run2.$now.sqlite $basedir/latest/run2/2x2runs_run2.latest.sqlite
    ln -sf ../../history/run2/2x2runs_run2.$now.xlsx $basedir/latest/run2/2x2runs_run2.latest.xlsx
    sleep 3600
done
