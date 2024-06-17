from ..DataManager import load_config
from ..DB import IFBeamManager

def get_beam_summary(start, end):
    config = load_config('config/IFbeam_parameters.yaml')
    manager = IFBeamManager(config)
    manager.set_time_range(start=start, end=end)
    pot, first_time, last_time = manager.get_total_pot(config['pot_device_name'])
    beam_data = {"Beam_summary":
                    {
                    "Total_POT": pot,
                    "Start": first_time,
                    "End": last_time
                    }
                }

    return beam_data
