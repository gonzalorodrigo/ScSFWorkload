from commonLib.DBManager import DB
from orchestration.definition import ExperimentDefinition

import os

db_obj  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))


there_are_more=True
ids = []
while there_are_more:
    ed_f = ExperimentDefinition()
    there_are_more  = ed_f.load_fresh(db_obj)
    if there_are_more:
        ids.append(ed_f._trace_id)

print("END2:", ids)
print("END3")