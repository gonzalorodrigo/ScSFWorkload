""" Plots boxplot analysis on the job slowdown of a set of experiment. Reads
data from a database. It reads data from 28 experiments and compares it with
another experiment considered to have the ideal slowdown. It assumes some
of the experiment conditions (time, algo, workflow, and edge keys). Usage:

python plot_exp_slowdown.py (first_exp_id) (no_slowdown_exp_id)

Args:
- first_exp_id: numeric id of the first experiment of the series.
- no_slowdown_exp_id: numeric id with the ideal "slowdown".


Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs.
"""


import sys

import matplotlib

from commonLib.nerscPlot import (paintHistogramMulti, paintBoxPlotGeneral,
                                 paintBarsHistogram)
from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace

matplotlib.use('Agg')

if len(sys.argv)<3:
    raise ValueError("At two integert argument must specified with the trace_id"
                     " to plot: first_exp_id, no_slowdown_exp_id")
first_id = int(sys.argv[1])
no_wf_slowdown_id=int(sys.argv[1])

last_id=first_id+27
time_keys={60:"60/h", 600:"6/h", 1800:"2/h", 3600:"1/h"}
algo_keys={"manifest":"wfware", "single":"single", "multi":"dep"}
workflow_keys={"floodplain.json":"floodP", "synthLongWide.json":"longWide",
               "synthWideLong.json":"wideLong"}
edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}




db_obj = get_central_db()

def get_slowdown(db_obj, trace_id):
    rt = ResultTrace()
    rt.load_trace(db_obj, trace_id)
    exp = ExperimentDefinition()
    exp.load(db_obj, trace_id)
    
    
    (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown, jobs_timesubmit) = (
                                    rt.get_job_times_grouped_core_seconds(
                                    exp.get_machine().get_core_seconds_edges(),
                                    exp.get_start_epoch(),
                                    exp.get_end_epoch(), True))
                
    return jobs_slowdown

jobs_slowdown={}
no_wf_slowdown=get_slowdown(db_obj, no_wf_slowdown_id)

for trace_id in range(first_id, 
                    last_id):
    print("TRACE_ID", trace_id)
    exp = ExperimentDefinition()
    exp.load(db_obj, trace_id)
    if not exp.is_it_ready_to_process():
        print("not ready")
        slowdown = {}
        for edge in edge_keys:
            slowdown[edge] = []
    else:
        slowdown = get_slowdown(db_obj, trace_id)
    
    for edge in slowdown:
        edge_key=edge_keys[edge]
        workflow_key=workflow_keys[exp._manifest_list[0]["manifest"]]
        time_key=time_keys[exp._workflow_period_s]
        algo_key=algo_keys[exp._workflow_handling]
        comb_key=time_key+"\n"+algo_key

        if edge_key not in jobs_slowdown.keys():
            jobs_slowdown[edge_key] = {}
        if workflow_key not in jobs_slowdown[edge_key].keys():
            jobs_slowdown[edge_key][workflow_key]={}
            jobs_slowdown[edge_key][workflow_key]["0/h"]=no_wf_slowdown[edge]
        jobs_slowdown[edge_key][workflow_key][comb_key]=slowdown[edge]
        

yLim={"[0,48] core.h": (0, 1000),
      "(48, 960] core.h":(0,100),
      "(960, inf.) core.h":(0,20)}

for edge in jobs_slowdown:

    paintBoxPlotGeneral("SlowDownCompare: {0}".format(edge),
                jobs_slowdown[edge], labelY="slowdow", 
                yLogScale=True,
                graphFileName="slowdown/firstcompare-boxplot-{0}".format(edge),
                yLim=yLim[edge]) 
