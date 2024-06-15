import requests
import pandas as pd
from ..DataManager import load_config

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'An error occurred: {e}')
        return None

def get_beam_data(start, end):
    config = load_config('config/IFbeam_parameters.yaml')
    bundle_url = f"https://{config['url']}v={config['pot_device_name']}&e={config['event']}&t0={start}&t1={end}&f=json"
    print(f"Fetching beam data for the time period {start} to {end}")

    bundle_data = fetch_data(bundle_url)
    if bundle_data:
        pot, first_time, last_time = get_beam_summary(bundle_data)
    else:
        print("WARNING: No beam data found!")
        pot, first_time, last_time = 0.0, start, end

    beam_data = {"Beam_summary":
                    {
                    "Total_POT": pot,
                    "Start": first_time,
                    "End": last_time
                    }
                }


    return beam_data

def get_beam_summary(data):
    if 'rows' in data:
        rows = data['rows']
    else: raise ValueError('No data rows found in beam data')

    df = pd.DataFrame(rows)
    total_value = df['value'].sum() #pot sum
    units = df['units']
    if not units.nunique() == 1:
        raise ValueError('Units in data are not consistent')
    unit = df.loc[0, 'units']
    pot = float(f"{total_value}{unit}")
    first_time = df.loc[0, 'time']
    last_time = df.loc[len(df) - 1, 'time']

    return pot, first_time, last_time
