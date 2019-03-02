""" This script creates an experiment set that runs for three hours: one
of pre-load, and two of actual trace. It does include workflows, 
and uses the fake model Edison (not actual Edison system).

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
""" 

from orchestration.definition import ExperimentDefinition
from orchestration import get_central_db

import sys

db_obj = get_central_db()

overload=1.0

if len(sys.argv)>=2:
    overload=float(sys.argv[1])

exp = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest":"floodplain.json"}],
                 workflow_policy="period",
                 workflow_period_s=300,
                 workflow_handling="manifest",
                 preload_time_s = 0,
                 workload_duration_s=3600*24*7,
                 overload_target=overload)
exp.store(db_obj)