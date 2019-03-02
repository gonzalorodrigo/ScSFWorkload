
"""UNIT TESTS 

 python -m unittest test_AnalysisRunner
 
 
"""

from commonLib.DBManager import DB
from stats.trace import ResultTrace
import datetime
import os
import unittest
from orchestration.definition import ExperimentDefinition
from orchestration.analyzing import AnalysisRunnerSingle
from stats import Histogram, NumericStats, NumericList
from commonLib.nerscUtilization import UtilizationEngine


class TestAnalysisRunnerSingle(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
        ht = Histogram()
        ht.create_table(self._db)
        self.addCleanup(self._del_table, ht._table_name)
        
        ns = NumericStats()
        ns.create_table(self._db)
        self.addCleanup(self._del_table, ns._table_name)
        
        us = NumericList("usage_values", ["utilization", "waste"])
        us.create_table(self._db)
        self.addCleanup(self._del_table, "usage_values") 
        
        rt = ResultTrace()
        self.addCleanup(self._del_table,"import_table" )
        rt.create_import_table(self._db, "import_table")
       
        self.addCleanup(self._del_table,"traces" )
        rt.create_trace_table(self._db, "traces")
        
        
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2],
             "account": ["account1", "account2"],
             "cpus_req": [48, 96],
             "cpus_alloc": [48, 96],
             "job_name":["jobName1", "jbname2"],
             "id_job": [1,2],
             "id_qos": [2,3],
             "id_resv": [3,4],
             "id_user": [4,5],
             "nodes_alloc": [2,4],
             "partition": ["partition1", "partition2"],
             "priority": [99, 199],
             "state": [3,2],
             "timelimit": [100,200],
             "time_submit": [3000,3001],
             "time_start": [3002,3001],
             "time_end": [3002,3005]                      
             }
        rt._lists_start = {
             "job_db_inx":[2,1],
             "account": ["account2","account1"],
             "cpus_req": [96, 48],
             "cpus_alloc": [96, 48],
             "job_name":["jobName2", "jobName1"],
             "id_job": [2,1],
             "id_qos": [3,2],
             "id_resv": [4,3],
             "id_user": [5,4],
             "nodes_alloc": [4,2],
             "partition": ["partition2","partition1"],
             "priority": [199, 99],
             "state": [2,3],
             "timelimit": [200,100],
             "time_submit": [3003,3000],
             "time_start": [3001,3002],
             "time_end": [3005,3002]                      
             }
        rt.store_trace(self._db, 1)
        self._rt=rt
        
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table `"+table_name+"`")
        self.assertTrue(ok, "Table was not created!")
    
    def test_setup(self):
        pass
    
    def test_load_trace(self):
        ed = ExperimentDefinition()
        ed._trace_id=1
        ar = AnalysisRunnerSingle(ed)
        new_rt=ar.load_trace(self._db)
        self.assertEqual(self._rt._lists_start, new_rt._lists_start)
        self.assertEqual(self._rt._lists_submit, new_rt._lists_submit)
        
    def test_do_full_analysis(self):   
        ed = ExperimentDefinition()
        ed._trace_id=1
        ed._start_date = datetime.datetime(1969,1,1)
        ed._workload_duration_s=365*24*3600
        ed._preload_time_s=0
        ar = AnalysisRunnerSingle(ed)
        ar.do_full_analysis(self._db)
        