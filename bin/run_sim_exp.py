"""
Runs simulation on single experiments on a worker vm which hostname
is specified as an input argument. It requires a database to store
the simulation results and to have access to the slurm database in the
worker. Database access data is configured through environment variables.

Experiment info is pulled from the central database. 

Usage:

python run_sim_exp-py [ip_of_simulator] [trace_id]

- ip_of_simulator: ip address of the machine running the simulation.
- trace_id: if set, it runs only the experiment with that trace_id, if not
    set, it will run all experiments in "fresh" state.

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the central database.
- ANALYSIS_DB_NAME: database name to write central to and read experiment info
    from.
- ANALYSIS_DB_USER: user to be used to access the central database.
- ANALYSIS_DB_PASS: password to be used to used to access the central database.
- ANALYSIS_DB_PORT: port on which the central database runs.
- SLURM_DB_NAME: slurm database name of the slurm worker. If not set takes
    slurm_acct_db.
- SLURMDB_USER: user to be used to access the slurm database.
- SLURMDB_PASS: password to be used torace used to access the slurm database.
- SLURMDB_PORT: port on which the slurm database runs.

 
"""
import os
import random
import sys
import time

from orchestration import ExperimentWorker
from orchestration import get_central_db, get_sim_db
from orchestration.running import ExperimentRunner


simulator_ip = "192.168.56.24"

if len(sys.argv)>=2:
    simulator_ip = sys.argv[1]
    

trace_id=None
if len(sys.argv)>=3:
    trace_id=sys.argv[2]

mysleep=os.getenv("SIM_MAX_WAIT", None)
if mysleep is not None:
    sleep_time=random.randrange(int(mysleep))
    print ("Doing a wait before starting ({0}): {1}s".format(simulator_ip, sleep_time))
    time.sleep(sleep_time)
    print ("Wait done, let's get started...")

ExperimentRunner.configure(
           trace_folder="/tmp/",
           trace_generation_folder=os.getenv("TRACES_TMP_DIR", "tmp"), 
           local=False,
           run_hostname=simulator_ip,
           run_user=None,
           scheduler_conf_dir="/scsf/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/scsf/",
           manifest_folder="manifests")
central_db_obj = get_central_db()
sched_db_obj = get_sim_db(simulator_ip)

ew = ExperimentWorker()

ew.do_work(central_db_obj, sched_db_obj, trace_id=trace_id)
