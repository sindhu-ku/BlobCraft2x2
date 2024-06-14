Author: Sindhujha Kumaran (s.kumaran@uci.edu)
## Query different subsystem databases and save them to JSON blobs
Pre-requisites:
- Fermilab VPN
- Tunnel into acd-daq05 for accessing influxdb: `ssh -L 8087:acd-daq05-priv:8086 acdcs@acd-gw05.fnal.gov`
- Get config/SC_credentials.yaml from me (make sure that the port in influxdb credentials is the same one as you tunneled into, eg. 8087 above)
- A LRS db file in config/LRS_parameters.yaml

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
```
### Slow/ detector controls

Usage:
```
SC_query --start="2024-05-27" --end="2024-05-28" --measurement="LAr_level"
```

This should produce a file of format `LAr_level_2024-05-27T00:00:00_2024-05-05:00-28T23:59:59.999999-05:00.json`

- required arguments for simple query:
  - --start: Start time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --end: End time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --measurement: Measurement name to query. Use 'all' if you want all the measurments inside SC_parameters/config.yaml (`influx_SC_DB`, `cryostat_tag_dict`, `purity_mon_variables`)
- optional:
  - --subsample: Subsample interval in s like '60S' if you want coarser measurements
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
    - set_voltage
    - oil_temperature
    - RTD_temperature

  - Cryo PSQL DB (more variables can be added in config/SC_parameters.yaml once you have the tagid for the measurement):
    - cryostat_pressure
    - LAr_level

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

- For influxDB measurements, add to `influx_SC_special_dict`. For example, `ground_impedance: ["gizmo", "resistance", ["resistance"]]`, here `ground_impedance` is the user-defined measurement name. Everything else is Slow Controls database-specific. `gizmo` is the database name, `resistance` is the measurement name, `resistance` is one of the variable names. You can also get multiple variables in the same measurement. Example, additionally `phase` in this case: `ground_impedance_phase: ["gizmo", "resistance", ["resistance", "phase"]]` or you can leave the variables field empty like this if you want all fields from the measurement:`ground_impedance_phase: ["gizmo", "resistance", []]`. Also see `influx_SC_data_dict` for more comprehensive measurements.
- For cryostat data, first obtain the tagid. Then edit `cryostat_tag_dict`. In this example, `cryostat_pressure: "34"`, the former is user-defined and the latter is the cryostat database-specific tagid
- All the purity monitor variables are already available in the config

### Light readout system

Usage:
```
LRS_query --run=<run_number>
```
Parameters that will be saved are in config/LRS_paramters.yaml and can be changed according to needs
