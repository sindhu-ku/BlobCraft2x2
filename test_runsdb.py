from Query2x2.LRS.LRS_query import LRS_blob_maker
from Query2x2.SC.SC_query import SC_blob_maker

subrun_dict = LRS_blob_maker(run=20, get_subrun_dict=True)
#print(subrun_dict)
#SC_blob_maker(measurement="runsdb", run=20, subrun_dict=subrun_dict) # this takes too much time because of too many queries (18-20s)

SC_blob_maker(start_time="2024-06-03 02:08:08", end_time="2024-06-03, 08:24:12", measurement="runsdb", run=20) #for the same start and end takes 3-5 seconds
