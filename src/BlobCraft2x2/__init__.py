from zoneinfo import ZoneInfo

import yaml

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

Global_config = load_config('config/Global_parameters.yaml')
CRS_config = load_config('config/CRS_parameters.yaml')
LRS_config = load_config('config/LRS_parameters.yaml')
Mx2_config = load_config('config/Mx2_parameters.yaml')
SC_config = load_config('config/SC_parameters.yaml')
IFbeam_config = load_config('config/IFbeam_parameters.yaml')

local_tz = ZoneInfo(Global_config['timezone'])
