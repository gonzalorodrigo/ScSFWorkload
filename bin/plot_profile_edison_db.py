"""
Plots the Histogram/CDF on job variables from the trace in database
in the NERSC torque format. Assumes the database is connectable local
and is called custom2. 

Env vars:
- NERSCDB_USER: user to be used to access the database.
- NERSCDB_PASS: password to be used to used to access the database.
"""
import datetime

from analysis import jobAnalysis
from plot import profile


start=datetime.date(2015, 1, 1)
end=datetime.date(2015, 1, 2)
print "LOADING DATA"
data_dic=jobAnalysis.get_jobs_data("edison", start.year, start.month, start.day,
                          end.year, end.month, end.day)
profile(data_dic["duration"], "Edison Logs\nJobs' wall clock (s)",
        "./graphs/edison-log-duration", "Wall clock (s)",
        x_log_scale=True)

profile(data_dic["totalcores"], "Edison Logs\nJobs' allocated number of cores",
        "./graphs/edison-log-cores", "Number of cores",
        x_log_scale=True)

profile(data_dic["wallclock_requested"], "Edison Logs\nJobs' wall clock limit(s)",
        "./graphs/edison-log-limit", "Wall clock limit(s)",
        x_log_scale=True)

inter_data = jobAnalysis.produce_inter_times(data_dic["created"])

profile(inter_data, "Edison Logs\nJobs' inter-arrival time(s)",
        "./graphs/edison-log-inter", "Inter-arrival time(s)",
        x_log_scale=True)
