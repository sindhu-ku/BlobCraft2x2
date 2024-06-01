Author: Sindhujha Kumaran (s.kumaran@uci.edu)

## Query databases and save them to JSON files

Pre-requisites: Fermilab VPN, Tunnel into acd-daq05, ask me for config/credentials.yaml (make sure to change the tunnel port for influxdb credentials)

Required packages: `sqlalchemy influxdb yaml datetime pytz pandas numpy json argparse dateutil`

```
cd SlowControls/
usage: python3 simple_query.py  --start="2024-05-27" --end="2024-05-28" --measurement="LAr_level"
```

This should produce a file of format `LAr_level_2024-05-27T00:00:00_2024-05-28T23:59:59.999999.json`

- required arguments:
  - --start: Start time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --end: End time for the query (various formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS.ssss')
  - --measurement: Measurement name to query
- optional:
  - --subsample: Subsample interval in s like '60S' if you want coarser measurements

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
