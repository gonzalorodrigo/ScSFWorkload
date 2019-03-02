"""
Creates the SQL schema for the workload databases.
 
Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
""" 

from orchestration import get_central_db

from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace
from stats import Histogram, NumericStats

db_obj = get_central_db()

ExperimentDefinition().create_table(db_obj)
ResultTrace().create_trace_table(db_obj, ResultTrace()._table_name)
Histogram().create_table(db_obj)
ResultTrace()._get_utilization_result().create_table(db_obj)

NumericStats().create_table(db_obj)

  
