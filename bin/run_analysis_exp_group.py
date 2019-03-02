"""
Runs analysis on group experiments that have been simulated but not
analyzed. Data is read and written to a database configured through
environment vars.

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
"""
from orchestration import AnalysisWorker
from orchestration import get_central_db
from orchestration.running import ExperimentRunner
import sys
ExperimentRunner.configure(
           trace_folder="/home/gonzalo/cscs14038bscVIII",
           trace_generation_folder="tmp", 
           local=False,
           run_user=None,
           scheduler_conf_dir="/home/gonzalo/cscs14038bscVIII/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/home/gonzalo/cscs14038bscVIII",
           manifest_folder="manifests")

trace_id=None
if len(sys.argv)>=2:
    trace_id=sys.argv[1]

central_db_obj = get_central_db()

ew = AnalysisWorker()

ew.do_work_grouped(central_db_obj, trace_id=trace_id)
