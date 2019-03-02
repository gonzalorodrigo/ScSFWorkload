"""
Connects to remote ScSF worker and pulls the jobs log.

Usage:

python ./rescue_exp_data.py (worker_vm_ip) (trace_id)

"""

import sys

from orchestration import ExperimentWorker
from orchestration import get_central_db, get_sim_db
from orchestration.running import ExperimentRunner


simulator_ip = "192.168.56.24"

if len(sys.argv)>=2:
    simulator_ip = sys.argv[1]
    

trace_id=None
if len(sys.argv)>=3:
    trace_id=sys.argv[2]
else:
    print("TRACE ID and VM IP are required to rescue data")

central_db_obj = get_central_db()
sched_db_obj = get_sim_db(simulator_ip)


ExperimentRunner.configure(
           trace_folder="/home/gonzalo/cscs14038bscVIII",
           trace_generation_folder="tmp", 
           local=False,
           run_hostname=simulator_ip,
           run_user=None,
           scheduler_conf_dir="/home/gonzalo/cscs14038bscVIII/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/home/gonzalo/cscs14038bscVIII",
           manifest_folder="manifests")
central_db_obj = get_central_db()
sched_db_obj = get_sim_db(simulator_ip)

ew = ExperimentWorker()

ew.rescue_exp(central_db_obj, sched_db_obj, trace_id=trace_id)