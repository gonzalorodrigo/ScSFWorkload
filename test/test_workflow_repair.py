from commonLib.DBManager import DB
from orchestration.running import ExperimentRunner
from stats.trace import ResultTrace
from stats.workflow_repair import StartTimeCorrector

import numpy as np
import os
import unittest
from orchestration.definition import ExperimentDefinition

class TestWorkflowRepair(unittest.TestCase):
    def setUp(self):
        ExperimentRunner.configure(manifest_folder="manifests")
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table `"+table_name+"`")
        self.assertTrue(ok, "Table was not created!")
    def _create_tables(self):
        rt = ResultTrace()
        self.addCleanup(self._del_table,"import_table" )
        rt.create_import_table(self._db, "import_table")
        self.addCleanup(self._del_table,"traces" )
        rt.create_trace_table(self._db, "traces")
        self.addCleanup(self._del_table,"experiment" )
        exp = ExperimentDefinition()
        exp.create_table(self._db)
        
    
    def test_get_workflow_info(self):
        stc = StartTimeCorrector()
        info=stc.get_workflow_info("synthLongWide.json")
        self.assertEqual(info["cores"],480)
        self.assertEqual(info["runtime"],18000)
        self.assertEqual(set(info["tasks"].keys()), set(["S0", "S1"]))
        
    def test_get_time_start(self):
        stc = StartTimeCorrector()
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1_S0",
                                      100000, "multi")
        self.assertEqual(new_start_time, 100000-14340)
        
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1_S1_dS0",
                                      100000, "multi")
        self.assertEqual(new_start_time, 100000-3540)
        
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1_S1_dS0",
                                      100000, "manifest")
        self.assertEqual(new_start_time, 100000-3540)
        
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1",
                                      100000, "manifest")
        self.assertEqual(new_start_time, 100000)
        
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1",
                                      100000, "single")
        self.assertEqual(new_start_time, 100000-18000)
        
        self.assertRaises(SystemError,
                          stc.get_time_start, "wf_synthLongWide.json-1",
                                      100000, "multi")
    
    def test_get_corrected_start_times(self):
        self._create_tables()
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "a3"],
             "cpus_req": [48, 96, 96],
             "cpus_alloc": [48, 96, 96],
             "job_name":["wf_synthLongWide.json-1_S0", 
                         "wf_synthLongWide.json-1_S1_dS0",
                         "wf_synthLongWide.json-2_S1_dS0"],
             "id_job": [1,2,3],
             "id_qos": [2,3,3],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,4],
             "partition": ["partition1", "partition2", "partition2"],
             "priority": [99, 199, 210],
             "state": [3,3, 3],
             "timelimit": [100,200, 300],
             "time_submit": [3000,3003, 3004],
             "time_start": [0,20000, 0],
             "time_end": [20000,25000, 30000]                      
             }
        trace_id=1
        rt.store_trace(self._db, trace_id)
        
        stc = StartTimeCorrector()
        stc._experiment = ExperimentDefinition()
        stc._experiment._trace_id=trace_id
        stc._trace=ResultTrace()
        stc._trace.load_trace(self._db, trace_id)
        new_times = stc.get_corrected_start_times("multi")
        self.assertEqual(new_times, {1:20000-14340, 3:30000-3540})
    
    def test_apply_new_times(self):
        self._create_tables()
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "a3"],
             "cpus_req": [48, 96, 96],
             "cpus_alloc": [48, 96, 96],
             "job_name":["wf_synthLongWide.json-1_S0", 
                         "wf_synthLongWide.json-1_S1_dS0",
                         "wf_synthLongWide.json-2_S1_dS0"],
             "id_job": [1,2,3],
             "id_qos": [2,3,3],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,4],
             "partition": ["partition1", "partition2", "partition2"],
             "priority": [99, 199, 210],
             "state": [3,3, 3],
             "timelimit": [100,200, 300],
             "time_submit": [3000,3003, 3004],
             "time_start": [0,20000, 0],
             "time_end": [20000,25000, 30000]                      
             }
        trace_id=1
        trace_id_orig=2
        rt.store_trace(self._db, trace_id)
        rt.store_trace(self._db, trace_id_orig)
        stc = StartTimeCorrector()
        stc._experiment = ExperimentDefinition()
        stc._experiment._trace_id=trace_id
        
        stc.apply_new_times(self._db,{1:20000-14340, 3:30000-3540})
        new_rt=ResultTrace()
        new_rt.load_trace(self._db, trace_id)
        self.assertEqual(new_rt._lists_submit["time_start"],
                         [20000-14340, 20000, 30000-3540])
        
        old_rt=ResultTrace()
        old_rt.load_trace(self._db, trace_id_orig)
        self.assertEqual(old_rt._lists_submit["time_start"],
                         [0,20000, 0])
        
    def test_correct_times(self):
        self._create_tables()
        exp = ExperimentDefinition(workflow_handling="manifest")
        trace_id=exp.store(self._db)
        rt=ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "a3"],
             "cpus_req": [48, 96, 96],
             "cpus_alloc": [48, 96, 96],
             "job_name":["wf_synthLongWide.json-1_S0", 
                         "wf_synthLongWide.json-1_S1_dS0",
                         "wf_synthLongWide.json-2"],
             "id_job": [1,2,3],
             "id_qos": [2,3,3],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,4],
             "partition": ["partition1", "partition2", "partition2"],
             "priority": [99, 199, 210],
             "state": [3,3, 3],
             "timelimit": [100,200, 300],
             "time_submit": [3000,3003, 3004],
             "time_start": [0,20000, 0],
             "time_end": [20000,25000, 30000]                      
             }
        
        rt.store_trace(self._db, trace_id)
        rt.store_trace(self._db, trace_id+1)
        stc = StartTimeCorrector()
        stc.correct_times(self._db, trace_id)
        
        new_rt=ResultTrace()
        new_rt.load_trace(self._db, trace_id)
        self.assertEqual(new_rt._lists_submit["time_start"],
                         [20000-14340, 20000, 30000])
        
        original_rt=ResultTrace()
        original_rt.load_trace(self._db, trace_id+1)
        self.assertEqual(original_rt._lists_submit["time_start"],
                         [0, 20000, 0])
        
        
        
        
        