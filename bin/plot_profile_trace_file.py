"""
Plots the Histogram/CDF on job variables from the trace file.

Usgges:

python ./plot_profile_trace_file.py trace_file.trace
"""
import sys

from analysis import jobAnalysis
from plot import profile


if len(sys.argv)<2:
    raise ValueError("At least one argument must specified with the file name "
                     "containing the trace.")


print("LOADING DATA")
data_dic=jobAnalysis.get_jobs_data_trace("./data/edison-1000jobs.trace")

profile(data_dic["duration"], "Trace\nJobs' wall clock (s)",
        "./graphs/trace-duration", "Wall clock (s)",
        x_log_scale=True)

profile(data_dic["totalcores"], "Trace\nJobs' allocated number of cores",
        "./graphs/trace-cores", "Number of cores",
        x_log_scale=True)

profile(data_dic["wallclock_requested"], "Trace\nJobs' "
        "wall clock limit(min)",
        "./graphs/trace-limit", "Wall clock limit(min)",
        x_log_scale=True)

inter_data = jobAnalysis.produce_inter_times(data_dic["created"])

profile(inter_data, "Trace\nJobs' inter-arrival time(s)",
        "./graphs/trace-inter", "Inter-arrival time(s)",
        x_log_scale=True)
