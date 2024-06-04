from Query2x2.LRS.LRS_query import LRS_blob_maker
from Query2x2.SC.SC_query import SC_blob_maker


subruns = LRS_blob_maker(run=20)

for subrun, times in subruns.items():
    SC_blob_maker(start=times['start_time'], end=times['end_time'], measurement="runsdb", run=20, subrun=subrun)
