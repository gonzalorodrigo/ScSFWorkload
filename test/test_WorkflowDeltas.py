
"""UNIT TESTS for the the class compareing workflows across traces.

 python -m unittest test_WorkflowDeltas
 
 
"""
import os
import unittest

from commonLib.DBManager import DB
from stats.compare import WorkflowDeltas
from stats import Histogram, NumericStats
from stats.trace import ResultTrace
from test_Result import assertEqualResult

class TestWorkflowDeltas(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created")
        
    def test_get_delta_values_same_format(self):
        job_list_1={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-3_S0", "wf_manifest-3_S1_dS0"],
                  "id_job":      [ 0,   1,   2,    3],
                  "time_submit": [ 100, 100, 1100, 1100],
                  "time_start":  [ 110, 215, 1200, 1400],
                  "time_end":    [ 200, 250, 1300, 1500]}
        
        job_list_2={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-3_S0", "wf_manifest-3_S1_dS0"],
                  "id_job":      [ 0,   1,   2,    3],
                  "time_submit": [ 100, 100, 1100, 1100],
                  "time_start":  [ 110, 600, 1200, 1900],
                  "time_end":    [ 200, 615, 1300, 2000]}
        
        
        wf_d = WorkflowDeltas()
        wf_d._first_trace=ResultTrace()
        wf_d._second_trace=ResultTrace()
        
        wf_d._first_trace._lists_submit = job_list_1
        wf_d._second_trace._lists_submit = job_list_2
        wf_d._first_workflows=wf_d._first_trace.do_workflow_pre_processing()
        wf_d._second_workflows=wf_d._second_trace.do_workflow_pre_processing()
        
        (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas) = wf_d.produce_deltas()
                 
        
        self.assertEqual(runtime_deltas, [365, 500])
        self.assertEqual(waitime_deltas, [0, 0])
        self.assertEqual(turnaround_deltas, [365, 500])
    
    def test_get_delta_values_different_format(self):
        job_list_1={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-3_S0", "wf_manifest-3_S1_dS0"],
                  "id_job":      [ 0,   1,   2,    3],
                  "time_submit": [ 100, 100, 1100, 1100],
                  "time_start":  [ 110, 215, 1200, 1400],
                  "time_end":    [ 200, 250, 1300, 1500]}
        
        job_list_2={"job_name":["wf_manifest-2_S0",
                              "wf_manifest-3_S0"],
                  "id_job":      [ 0,   1],
                  "time_submit": [ 100, 1100],
                  "time_start":  [ 110, 1200],
                  "time_end":    [ 615, 2000]}
        
        
        wf_d = WorkflowDeltas()
        wf_d._first_trace=ResultTrace()
        wf_d._second_trace=ResultTrace()
        
        wf_d._first_trace._lists_submit = job_list_1
        wf_d._second_trace._lists_submit = job_list_2
        wf_d._first_workflows=wf_d._first_trace.do_workflow_pre_processing()
        wf_d._second_workflows=wf_d._second_trace.do_workflow_pre_processing()
        
        (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas) = wf_d.produce_deltas()
                 
        
        self.assertEqual(runtime_deltas, [365, 500])
        self.assertEqual(waitime_deltas, [0, 0])
        self.assertEqual(turnaround_deltas, [365, 500])  
    
    def test_get_delta_values_append(self):
        job_list_1={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-3_S0", "wf_manifest-3_S1_dS0"],
                  "id_job":      [ 0,   1,   2,    3],
                  "time_submit": [ 100, 100, 1100, 1100],
                  "time_start":  [ 110, 215, 1200, 1400],
                  "time_end":    [ 200, 250, 1300, 1500]}
        
        job_list_2={"job_name":["wf_manifest-2_S0",
                              "wf_manifest-3_S0"],
                  "id_job":      [ 0,   1],
                  "time_submit": [ 100, 1100],
                  "time_start":  [ 110, 1200],
                  "time_end":    [ 615, 2000]}
        
        
        wf_d = WorkflowDeltas()
        wf_d._first_trace=ResultTrace()
        wf_d._second_trace=ResultTrace()
        
        wf_d._first_trace._lists_submit = job_list_1
        wf_d._second_trace._lists_submit = job_list_2
        wf_d._first_workflows=wf_d._first_trace.do_workflow_pre_processing()
        wf_d._second_workflows=wf_d._second_trace.do_workflow_pre_processing()
        
        (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas) = wf_d.produce_deltas()
        
        self.assertEqual(runtime_deltas, [365, 500])
        self.assertEqual(waitime_deltas, [0, 0])
        self.assertEqual(turnaround_deltas, [365, 500])
                 
        job_list_3={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-3_S0", "wf_manifest-3_S1_dS0"],
                  "id_job":      [ 0,   1,   2,    3],
                  "time_submit": [ 100, 100, 1100, 1100],
                  "time_start":  [ 110, 215, 1200, 1400],
                  "time_end":    [ 200, 250, 1300, 1500]}
        
        job_list_4={"job_name":["wf_manifest-2_S0",
                              "wf_manifest-3_S0"],
                  "id_job":      [ 0,   1],
                  "time_submit": [ 100, 1100],
                  "time_start":  [ 110, 1200],
                  "time_end":    [ 615, 2000]}
        
        wf_d._first_trace=ResultTrace()
        wf_d._second_trace=ResultTrace()
        wf_d._first_trace._lists_submit = job_list_3
        wf_d._second_trace._lists_submit = job_list_4
        wf_d._first_workflows=wf_d._first_trace.do_workflow_pre_processing()
        wf_d._second_workflows=wf_d._second_trace.do_workflow_pre_processing()
        
        (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas) = wf_d.produce_deltas(append=True)
        
        self.assertEqual(runtime_deltas, [365, 500, 365, 500])
        self.assertEqual(waitime_deltas, [0, 0, 0 , 0])
        self.assertEqual(turnaround_deltas, [365, 500, 365, 500])  
        
    def test_store_load(self):
        db_obj = self._db
        hist = Histogram()
        stat = NumericStats()
        self.addCleanup(self._del_table,"histograms")
        self.addCleanup(self._del_table,"numericStats")
        hist.create_table(db_obj)
        stat.create_table(db_obj)
        
        job_list_1={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-3_S0", "wf_manifest-3_S1_dS0"],
                  "id_job":      [ 0,   1,   2,    3],
                  "time_submit": [ 100, 100, 1100, 1100],
                  "time_start":  [ 110, 215, 1200, 1400],
                  "time_end":    [ 200, 250, 1300, 1500]}
        
        job_list_2={"job_name":["wf_manifest-2_S0",
                              "wf_manifest-3_S0"],
                  "id_job":      [ 0,   1],
                  "time_submit": [ 100, 1100],
                  "time_start":  [ 110, 1200],
                  "time_end":    [ 615, 2000]}
        
        
        wf_d = WorkflowDeltas()
        wf_d._first_trace=ResultTrace()
        wf_d._second_trace=ResultTrace()
        
        wf_d._first_trace._lists_submit = job_list_1
        wf_d._second_trace._lists_submit = job_list_2
        wf_d._first_workflows=wf_d._first_trace.do_workflow_pre_processing()
        wf_d._second_workflows=wf_d._second_trace.do_workflow_pre_processing()
        wf_d.produce_deltas()
        results_1 = wf_d.calculate_delta_results(True, db_obj, 1)
        
        wf_d_2 = WorkflowDeltas()
        results_2 = wf_d_2.load_delta_results(db_obj, 1)
        
        for field in results_1.keys():
            assertEqualResult(self, results_1[field], 
                              results_2[field], field)
        
