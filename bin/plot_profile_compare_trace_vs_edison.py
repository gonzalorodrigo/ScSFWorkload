"""
Calculates and compares the histogram/CDF for job variables of a database
trace and a synthetic trace file.   
"""

# remote use no Display
import datetime

import matplotlib

from analysis import jobAnalysis
from plot import profile_compare


matplotlib.use('Agg')




start=datetime.date(2015, 1, 1)
end=datetime.date(2015, 12, 31)
print("LOADING DATA from trace")
trace_dic=jobAnalysis.get_jobs_data_trace("./data/edison-1Year-2015.trace")

print("LOADING DATA from Database")
logs_dic=jobAnalysis.get_jobs_data("edison", start.year, start.month, start.day,
                          end.year, end.month, end.day)


print("Plotting...")

core_hours_logs = [float(x*y)/3600.0 for (x,y) in
                    zip(logs_dic["duration"], logs_dic["totalcores"])]
core_hours_trace = [float(x*y)/3600.0 for (x,y) in
                    zip(trace_dic["duration"], trace_dic["totalcores"])]

profile_compare(
        core_hours_logs, core_hours_trace,
        "Edison logs vs. synth\nJobs' core-hours",
        "./graphs/edison-compare-corehours", "core hours (core*h)",
        x_log_scale=True, filterCut=100000)

profile_compare(
        logs_dic["duration"], trace_dic["duration"],
        "Edison logs vs. synth\nJobs' wall clock (s)",
        "./graphs/edison-compare-duration", "Wall clock (s)",
        x_log_scale=True, filterCut=1000000)

profile_compare(
        logs_dic["totalcores"], trace_dic["totalcores"],
        "Edison logs vs. synth\nJobs' allocated number of cores",
        "./graphs/edison-compare-cores", "Number of cores",
        x_log_scale=True, filterCut=100000)

trace_dic["wallclock_requested"] = [x*60 for x in 
                                        trace_dic["wallclock_requested"]]
profile_compare(
        logs_dic["wallclock_requested"], trace_dic["wallclock_requested"],
        "Edison logs vs. synth\nJobs' wall clock limit(s)",
        "./graphs/edison-compare-limit", "Wall clock limit(s)",
        x_log_scale=True, filterCut=1000000)

inter_data_logs = jobAnalysis.produce_inter_times(logs_dic["created"])
inter_data_trace = jobAnalysis.produce_inter_times(trace_dic["created"])

profile_compare(inter_data_logs,inter_data_trace,
         "Edison logs vs. synth\nJobs' inter-arrival time(s)",
        "./graphs/edison-compare-inter", "Inter-arrival time(s)",
        x_log_scale=False, filterCut=20)
