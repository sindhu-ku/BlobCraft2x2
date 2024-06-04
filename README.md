Author: Sindhujha Kumaran (s.kumaran@uci.edu)

## Query 2x2 detector controls databases and save them to JSON files

Pre-requisites:
- Fermilab VPN
- Tunnel into acd-daq05 for accessing influxdb: `ssh -L 8087:acd-daq05-priv:8086 acdcs@acd-gw05.fnal.gov`
- Get config/credentials.yaml from me (make sure the port in influxdb credentials is the same one as you tunneled into, eg. 8087 above)

Installation:
```
git clone https://github.com/sindhu-ku/2x2_RunsDB.git
cd 2x2_RunsDB/DetectorControlsQuery/
```
Do a simple pip install or setup a virtual environment:
```
pip3 install .
```
(or)
```
python -m venv detcontrolsquery.venv
source detcontrolsquery.venv/bin/activate
pip install --upgrade pip setuptools wheel
```
Usage:
```
query --start="2024-05-27" --end="2024-05-28" --measurement="LAr_level"
```

This should produce a file of format `LAr_level_2024-05-27T00:00:00_2024-05-28T23:59:59.999999.json`

- required arguments:
  - --start: Start time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --end: End time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --measurement: Measurement name to query. Use 'runsdb' for runs database and 'all' if you want all the measurments inside parameters/config.yaml (`influx_SC_data_dict`, `cryostat_tag_dict`, `purity_mon_variables`)
  - --run: Run number for runsdb (required when measurement is runsdb)
- optional:
  - --subsample: Subsample interval in s like '60S' if you want coarser measurements
  - --subrun: Subrun number for runsdb (optional)

### Currently supported measurements:
  - InfluxDB (more variables can be easily added in config/parameters.yaml):
    - ground_impedance
    - pick_off_voltages
    - set_voltage
    - oil_temperature
    - RTD_temperature

  - Cryo PSQL DB (more variables can be added in config/parameters.yaml once you have the tagid for the measurement):
    - cryostat_pressure
    - LAr_level

  - Purity monitor measurements PSQL DB:
    - electron_lifetime
    - impurities
    - anode_peak
    - cathode_peak
    - anode_time
    - cathode_time
    - anode_hv
    - cathode_hv
    - anodegrid_hv

### Adding custom measurements in config/parameters.yaml

- For influxDB measurements, add to `influx_SC_special_dict`. For example, `ground_impedance: ["gizmo", "resistance", ["resistance"]]`, here `ground_impdance` is the user-defined measurement name. Everything else is Slow Controls database-specific. `gizmo` is the database name, `resistance` is the measurement name, `resistance` is one of the variable names. You can also get multiple variables in the same measurement. Example, additionally `phase` in this case: `ground_impedance_phase: ["gizmo", "resistance", ["resistance", "phase"]]`. Also see `influx_SC_data_dict` for more comprehensive measurements
- For cryostat data, first obtain the tagid. Then edit `cryostat_tag_dict`. In this example, `cryostat_pressure: "34"`, the former is user-defined and the latter is the cryostat database-specific tagid
- All the purity monitor variables are already available in the config
