"""
Generates the trace file for an experiment identified by trace_id. Stores the
resulting trace file in tmp/[name], with the name of the experiment.

usage:

python generate_trace.pt trace_id

trace_id: numeric id of the experiment.
"""

from orchestration import ExperimentDefinition
from orchestration.running import ExperimentRunner
from orchestration import get_central_db

import sys
    

trace_id=None
if len(sys.argv)>=2:
    trace_id=sys.argv[1]
else:
    print "Missing experiment trace_id."
    exit()
    
ExperimentRunner.configure(
           trace_folder="/home/gonzalo/cscs14038bscVIII",
           trace_generation_folder="tmp", 
           local=False,
           run_user=None,
           scheduler_conf_dir="/home/gonzalo/cscs14038bscVIII/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/home/gonzalo/cscs14038bscVIII",
           manifest_folder="manifests")


central_db_obj = get_central_db()

ed = ExperimentDefinition()
ed.load(central_db_obj, trace_id)

er = ExperimentRunner(ed)
er._generate_trace_files(ed)
