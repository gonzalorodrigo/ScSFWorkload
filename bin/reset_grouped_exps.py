"""
Resest the state of experiments in the database

Usage:
python reset_exps.py [state_to_be_reseted] [new_state] [trace_id]

Args:
- state_to_be_reseted: state all experiments in this state will be reseted.
    If not set, experiments in "pre_simulating" are reseted.
- new_state: new state of the experiments. If new_state is not set, experiments
    are set to fresh.If new state is set to fresh, existing traces and
    experiment results are cleaned. If new state is simulation_complete, only
    results are cleaned.
- trace_id: numeric id of the trace to be reseted. If set, only the trace with
    such id will be reseted. 

Valid state values: "fresh", "pre_simulating", "simulating", "simulation_done",
"simulation_error", "pre_analyzing", "analyzing", "analysis_done",
"analysis_error".

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
"""

import sys

from orchestration import get_central_db, get_sim_db
from orchestration.definition import GroupExperimentDefinition


state="analysis_done"
new_state="pending"
if len(sys.argv)>=2:
    state = sys.argv[1]
if len(sys.argv)>=3:
    new_state = sys.argv[2]
if len(sys.argv)==4:
    trace_id=int(sys.argv[3])
else:
    trace_id=None


central_db_obj = get_central_db()

print("Reseting experiments in state {0}".format(state))
there_are_more = True
while (there_are_more):
    ed = GroupExperimentDefinition()
    if trace_id is not None:
        ed.load(central_db_obj, trace_id)
        if ed._work_state!=state:
            print("Error, unexpected state {0} for trace {1}".format(
                                                             ed._work_state,
                                                             trace_id))
            exit()
        ed.upate_state(central_db_obj, new_state)
        there_are_more=False
    else:
        there_are_more=ed.load_next_state(central_db_obj, state, new_state)
    if ed._trace_id!=None:
        print("Reset experiment({0}: {1}): {2} -> {3}".format(ed._trace_id,
                                                  ed._name,
                                                  state,
                                                  new_state))
        if new_state == "fresh":
            ed.del_trace(central_db_obj)
            ed.update_worker(central_db_obj,"")
            ed.reset_simulating_time(central_db_obj)
        if new_state in ["fresh", "simulation_complete", "simulation_done",
                         "pending"]:
            print("A resetear")
            ed.del_results(central_db_obj)
        if new_state in ["analysis_done"]:
            ed.del_results_like(central_db_obj)