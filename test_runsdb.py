from Query2x2.LRS.LRS_query import LRS_blob_maker
from Query2x2.SC.SC_query import SC_blob_maker

subrun_dict = LRS_blob_maker(run=20, get_subrun_dict=True) #gets subrun_dict but only dumps summary info

LRS_blob_maker(run=20, get_subrun_dict=False, dump_all_data=True)   #dumps all tables in LRS DB

SC_blob_maker(measurement_name="runsdb", run_number=20, subrun_dict=subrun_dict) #only dumps summary SC data of LRS subruns

SC_blob_maker(measurement_name="all", run_number=20, subrun_dict=subrun_dict) #dumps all timeseries SC data of LRS subruns
