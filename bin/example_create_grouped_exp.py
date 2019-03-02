
""" This script creates an experiment set that runs for three hours: one
of pre-load, and two of actual trace. It does not include workflows, 
and uses the fake model Edison (not actual Edison system).

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
""" 

from orchestration.definition import GroupExperimentDefinition
from orchestration import get_central_db

import sys

db_obj = get_central_db()
""" Fixed vars"""
overload=1.0
machine_name="edison"
if len(sys.argv)>=2:
    overload=float(sys.argv[1])

preload_time=3600*24*1
experiment_time=3600*24*5


trace_id_list=[182, 237, 292 ,347, 457]

exp = GroupExperimentDefinition(
             seed="GROUPED",
             machine=machine_name,
             trace_type="group",
             manifest_list=[{"share": 1.0, "manifest":"floodplain.json"}],
             workflow_policy="period",
             workflow_period_s=600,
             workflow_handling="multi",
             preload_time_s = preload_time,
             workload_duration_s=experiment_time,
             overload_target=overload,
             subtraces=trace_id_list)
exp.store(db_obj)