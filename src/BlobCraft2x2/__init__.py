from zoneinfo import ZoneInfo

import yaml


local_tz = ZoneInfo('America/Chicago')

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

CRS_config = load_config('config/CRS_parameters.yaml')
LRS_config = load_config('config/LRS_parameters.yaml')
Mx2_config = load_config('config/Mx2_parameters.yaml')
SC_config = load_config('config/SC_parameters.yaml')
IFbeam_config = load_config('config/IFbeam_parameters.yaml')
