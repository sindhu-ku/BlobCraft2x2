Authors: Sindhujha Kumaran (s.kumaran@uci.edu), Matt Kramer (mkramer@lbl.gov)
## Query different subsystem databases and save them to JSON blobs
Pre-requisites (the VPN, tunnel and credentials are only needed for slow controls, `SC_query`):
- Fermilab VPN
- Tunnel into acd-daq05 for accessing influxdb: `ssh -L 8087:acd-daq05-priv:8086 acdcs@acd-gw05.fnal.gov` (make sure you have an account on acdcs)
- Get `SC_credentials.yaml` from Sindhujha Kumaran or Matt Kramer and put it in the config directory (e.g. `configs/2x2/`, see below). Make sure that the port in the influxdb credentials is the same one as you tunneled into, e.g. 8087 above
- A LRS db file in config/LRS_parameters.yaml and a Mx2 db file in config/Mx2_parameters.yaml

Installation:
```
git clone https://github.com/sindhu-ku/BlobCraft2x2.git
cd BlobCraft2x2/
```
Do a simple pip install or setup a virtual environment:
```
pip3 install .
```
(or)
```
python -m venv BlobCraft2x2.venv
source BlobCraft2x2.venv/bin/activate
pip install --upgrade pip setuptools wheel
pip3 install .
```

Symlink the appropriate config directory:
- For 2x2: `ln -s configs/2x2 config`
- For FSD: `ln -s configs/FSD config`

Place a copy/symlink of the MORCS database at `config/morcs.sqlite`.

Run the queries from the directory that contains `config`, since config files are read from `config/...`.

`connect_timeout` in `config/Global_parameters.yaml` (default 20 s) is the connection timeout for the network databases (PostgreSQL, InfluxDB, IFbeam), so an unreachable database gives an error instead of hanging. It only limits connecting, not long queries. Each database is only connected to when a query needs it.

### Generating a full runs DB

Running e.g. `scripts/make_db.sh 20014` will generate `output/runs_20014.db`. The script `scripts/merge_sqlite.py` can then be used to merge these. For example, to produce `runs_all.db` from runs 20014 and 20015:

``` bash
scripts/make_db.sh 20014
scripts/make_db.sh 20015
scripts/merge_sqlite.py runs_all.db output/runs_*.db
```

The rest of this readme describes the individual subsystems.

### Slow/ detector controls

Usage:
```
SC_query --start="2024-05-27" --end="2024-05-28" --measurement="LAr_level_mm"
```

This should produce a file of format `LAr_level_mm_2024-05-27T00:00:00_2024-05-05:00-28T23:59:59.999999-05:00.json`

- required arguments for simple query:
  - --start: Start time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --end: End time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --measurement: Measurement name to query. Use 'all' if you want all the measurments inside SC_parameters/config.yaml (`influx_SC_DB`, `cryostat_tag_dict`, `purity_mon_variables`)
- optional:
  - --subsample: Subsample interval in s like '60s' if you want coarser measurements (lowercase 's'; newer pandas versions reject '60S')
  - --output_dir: Directory to save the output files. Default is current directory
- runsDB specific:
  - --measurement: 'runsdb' for summary and 'ucondb' for dumping all measurements in one json blob
  - --run: Run number for runsdb (required when measurement is runsdb)
  - --subrun: Subrun number for runsdb
  - --subrun_dict: Dictionary of subruns with start and end times. Required for runsdb if start and end are not given

#### Currently supported individual measurements (Note: option "all" dumps everything independent of below):
  - InfluxDB (more variables can be easily added in config/SC_parameters.yaml):
    - ground_impedance
    - pick_off_voltages
    - set_voltage (read from psql from run-2)
    - oil_temperature
    - RTD_temperature
    - cryostat_pressure (read from psql after 2024-08-22)
    - LAr_level_mm (read from psql after 2024-08-22)
    - O2_ppb (read from psql after 2024-08-22)

  - Purity monitor measurements PSQL DB (note simply giving "purity_monitor" will dump all the below variables):
    - electron_lifetime
    - impurities
    - anode_peak
    - cathode_peak
    - anode_time
    - cathode_time
    - anode_hv
    - cathode_hv
    - anodegrid_hv

#### Adding custom measurements in config/SC_parameters.yaml

- For influxDB measurements, add to `influx_SC_special_dict`:
  - For example, `ground_impedance: ["gizmo", "resistance", ["resistance"]]`, here `ground_impedance` is the user-defined measurement name. Everything else is Slow Controls database-specific. `gizmo` is the database name, `resistance` is the measurement name, `resistance` is one of the variable names.
  - You can also get multiple variables in the same measurement. Example, additionally `phase` in this case: `ground_impedance_phase: ["gizmo", "resistance", ["resistance", "phase"]]`.
  - Or you can leave the variables field empty like this if you want all fields from the measurement:`ground_impedance_phase: ["gizmo", "resistance", []]`.
  - Also see `influx_SC_data_dict` for more comprehensive measurements.
- All the purity monitor variables are already available in the config

### Beam information

Usage:
```
Beam_query --start=<start> --end=<end> --measurement="Total POT"
```
Similar to SC query for times. Supported measurements: "Total POT" (total for a given time) and "POT" (full timeseries).

POT is read from the devices in `pot_device_names` in `config/IFbeam_parameters.yaml` (`E:TRTGTD`, then `E:TOR101`). As in ND_CAFMaker, each spill uses the first device with a reading above `pot_threshold`, so spills where `E:TRTGTD` is missing or below threshold are taken from `E:TOR101`; spills below threshold on both devices are not counted. Readings from different devices less than `spill_match_dt` (0.1 s) apart belong to the same spill (the devices differ by up to ~15 ms and spills are ~1.2 s apart). The POT timeseries has a `device` field per spill, and the beam summary (also in the runs DB as `beam_summary_POT_device`) has `POT_device`, listing the devices that were used.

To use only one device, e.g. for detailed checks:
```
Beam_query --start=<start> --end=<end> --measurement="Total POT" --device="E:TOR101"
```

### Charge readout system

Usage:
```
CRS_query --run=<run_number>
```
Parameters that will be saved are in config/CRS_paramters.yaml and can be changed according to needs

### Light readout system

Usage:
```
LRS_query --run=<run_number>
```
Parameters that will be saved are in config/LRS_paramters.yaml and can be changed according to needs

### Mx2 readout system

Usage:
```
Mx2_query --run=<run_number>
```
All meta information in the db file is saved
