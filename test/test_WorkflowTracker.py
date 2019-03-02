"""UNIT TESTS for the the class extractig workflows from the traces

 python -m unittest test_WorkflowTracker
 
 
"""
from commonLib.DBManager import DB
from commonLib.nerscUtilization import _apply_deltas_usage
import os
from stats import Histogram, NumericStats
from stats.workflow import TaskTracker, WorkflowTracker, WorkflowsExtractor, \
    WasteExtractor, _fuse_delta_lists
import unittest

from test_Result import assertEqualResult
from test_ResultTrace import FakeDBObj


class TestWorkflowsExtractor(unittest.TestCase):
    
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
    
    def test_extract(self):
        job_list={"job_name":["wf_manifest-2",
                      "wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [2, 1,  0,  55,  4,  5,  6, 7,
                         8,
                         3, 9, 10, 11, 12],
          "time_start": [ 1, 2, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 2, 15, 17, 22],
          "time_end":   [2, 10, 20, 40, 25, 29, 50, 70,
                         30,
                         2, 10, 20, 19, 25]}
        we = WorkflowsExtractor()
        
        we.extract(job_list)
        
        self.assertEqual(len(we._workflows), 2)
        self.assertEqual(list(we._workflows.keys()), ["manifest-2", "manifest-3"])
        self.assertEqual(len(we._workflows["manifest-2"]._tasks), 7)
        self.assertEqual(we._workflows["manifest-2"]._parent_job.name, 
                         "wf_manifest-2")
        self.assertEqual(len(we._workflows["manifest-3"]._tasks), 4)
    
    def test_extract_process(self):
        job_list={"job_name":["wf_manifest-2", "wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [ 2, 0,  1,  33,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11],
          "time_start": [ 1, 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22],
          "time_end":   [1,10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25]}
        we = WorkflowsExtractor()
        
        we.extract(job_list)
        we.do_processing()
        
        self.assertEqual(len(we._workflows), 2)
        self.assertEqual(list(we._workflows.keys()), ["manifest-2", "manifest-3"])
        
        wt=we.get_workflow("manifest-3")
        t0 = wt._tasks["S0"]
        t1 = wt._tasks["S1"]
        t2 = wt._tasks["S2"]
        t3 = wt._tasks["S3"]
        self.assertEqual(wt._start_task,t0)
        self.assertEqual(wt._critical_path, [t0,t1,t3])
        self.assertEqual(wt._critical_path_runtime, 24)           
        wt=we.get_workflow("manifest-2")
        t0 = wt._tasks["S0"]
        t6 = wt._tasks["S6"]
        
        self.assertEqual(wt._start_task,t0)
        self.assertEqual(wt._critical_path, [t0,t6])
        self.assertEqual(wt._critical_path_runtime, 69)
        
    
    def test_extract_process_wrong_dash_name(self):
        job_list={"job_name":[ "wf_floodplain.json-350",
                              "wf_floodplain.json-350_S0",         
                              "wf_floodplain.json-350_S1",      
                              "wf_floodplain.json-350_S3-dS0-dS1",
                              "wf_floodplain.json-350_S5-dS0-dS1",
                              "wf_floodplain.json-350_S2-dS3-dS0", 
                              "wf_floodplain.json-350_S4-dS0-dS5", 
                              "wf_floodplain.json-350_S6-dS4-dS2"], 
          "id_job":     [ 39794,
                         39796,
                         39797,
                         39798,
                         39799,
                         39800,
                         39801,
                         39802],
          "time_submit": [1420309202,
                          1420309202,
                          1420309202,
                          1420309202,
                          1420309202,
                          1420309202,
                          1420309202,
                          1420309202],
          "time_start": [ 1420318973,
                            1420318973,
                            1420318973,
                            1420358574,
                            1420358574,
                            1420387379,
                            1420405383,
                            1420419788],
          "time_end":   [1420318973,
                        1420358573,
                        1420322573,
                        1420387374,
                        1420405374,
                        1420398179,
                        1420419784,
                        1420435988]}
        we = WorkflowsExtractor()
        
        we.extract(job_list)
        we.do_processing()
        
        self.assertEqual(len(we._workflows), 1)
        self.assertEqual(list(we._workflows.keys()), ["floodplain.json-350"])
        
        wt=we.get_workflow("floodplain.json-350")
        print("TASKS", list(wt._tasks.keys()))
        print("DEPS", [t.deps for t in list(wt._tasks.values())])
        print("CP", [x.name for x in wt._critical_path])
        self.assertEqual(wt.get_runtime(),117015)
        self.assertEqual(wt.get_waittime(), 9771)
        self.assertEqual(wt.get_turnaround(), 9771+117015)           
        
        
    def test_extract_process_single(self):
        db_obj = FakeDBObj(self)
        job_list={"job_name":["wf_manifest-0", 
                      "wf_manifest-1", "sim_job"],
          "id_job":     [ 0,  1,  2],
          "time_submit": [ 1, 3, 4],
          "time_start": [ 1, 15, 17],
          "time_end":   [11, 0, 40],
          "cpus_alloc": [100, 100, 300]}
        we = WorkflowsExtractor()
        
        we.extract(job_list)
        we.do_processing()
        
        self.assertEqual(len(we._workflows), 1)
        self.assertEqual(list(we._workflows.keys()), ["manifest-0"])
        
        wt=we.get_workflow("manifest-0")
        t0 = wt.get_all_tasks()[0]
        #t1 = wt._tasks["S1"]
        #t2 = wt._tasks["S2"]
        #t3 = wt._tasks["S3"]
        self.assertEqual(wt._start_task,t0)
        self.assertEqual(wt._critical_path, [t0])
        self.assertEqual(wt._critical_path_runtime, 10)   
        we.calculate_overall_results(True, db_obj, 1)
        
        
    def test_calculate_job_results(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4]}
        
        
        we.extract(job_list)
        we.do_processing()
        
        we.calculate_overall_results(True, db_obj, 1)
        
        self.assertEqual(db_obj._id_count, 12)
        self.assertEqual(db_obj._set_fields,
            ["wf_runtime_cdf", "wf_runtime_stats", "wf_waittime_cdf",
             "wf_waittime_stats", "wf_turnaround_cdf", 
             "wf_turnaround_stats", "wf_stretch_factor_cdf", 
             "wf_stretch_factor_stats", "wf_jobs_runtime_cdf",
             "wf_jobs_runtime_stats", "wf_jobs_cores_cdf", 
             "wf_jobs_cores_stats"])
        self.assertEqual(db_obj._hist_count, 6)
        self.assertEqual(db_obj._stats_count, 6)
    
    def test_fill_overall_values(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2", "wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [2, 0,  1,  33,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11],
          "time_start": [2, 2, 15, 17, 22, 27, 42, 12,
                         20,
                         2, 15, 17, 22],
          "time_end":   [2, 10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25],
          "time_submit": [1, 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2],
          "cpus_alloc": [33,  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4]}
        
        
        we.extract(job_list)
        we.do_processing()
        we.fill_overall_values()
        self.assertEqual(we._wf_runtime, [68,23])
        self.assertEqual(we._wf_waittime, [1,0])
        self.assertEqual(we._wf_turnaround, [69,23])
        self.assertEqual(len(we._wf_stretch_factor),2)
        self.assertEqual(len(we._wf_jobs_runtime),11)
        self.assertEqual(len(we._wf_jobs_cores), 11)
        
        we.extract(job_list)
        we.do_processing()
        we.fill_overall_values(append=True)
        self.assertEqual(we._wf_runtime, [68,23, 68, 23])
        self.assertEqual(we._wf_waittime, [1,0,1,0])
        self.assertEqual(we._wf_turnaround, [69,23,69,23])
        self.assertEqual(len(we._wf_stretch_factor),4)
        self.assertEqual(len(we._wf_jobs_runtime),22)
        self.assertEqual(len(we._wf_jobs_cores), 22)
        
    
    def test_get_workflow_times_start_stop(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4]}
        
   
        we.extract(job_list)
        we.do_processing()
        (wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores) = we._get_workflow_times(
                                            submit_start=2,
                                            submit_stop=3)
        
        self.assertEqual(wf_runtime, [24])
    
    
    def test_get_workflow_times_start_stop_per_manifest(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4]}
        
   
        we.extract(job_list)
        we.do_processing()
        manifests = we._get_per_manifest_workflow_times(
                                            submit_start=2,
                                            submit_stop=3)
        
        self.assertEqual(manifests["manifest"]["wf_runtime"], [24])

    
    def test_get_workflow_times_start_stop_per_manifest_multi(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2",
                      "wf_manifestA-4_S0"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11,
                         14],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22,
                         4],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25,
                         10],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2,
                           3],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4,
                           1]}
        
   
        we.extract(job_list)
        we.do_processing()
        manifests = we._get_per_manifest_workflow_times(
                                            submit_start=2,
                                            submit_stop=None)
        
        self.assertEqual(manifests["manifest"]["wf_runtime"], [24])
        self.assertEqual(manifests["manifestA"]["wf_runtime"], [6])
    
    def test_load_job_results(self):
        db_obj = self._db
        hist = Histogram()
        stat = NumericStats()
        self.addCleanup(self._del_table,"histograms")
        self.addCleanup(self._del_table,"numericStats")
        hist.create_table(db_obj)
        stat.create_table(db_obj)
        
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4]}
        
        
        we.extract(job_list)
        we.do_processing()
        
        old_results=we.calculate_overall_results(True, db_obj, 1)
        
        new_we = WorkflowsExtractor()
        new_results=new_we.load_overall_results(db_obj, 1)
        for field in ["wf_runtime_cdf", "wf_runtime_stats",
             "wf_waittime_cdf",
             "wf_waittime_stats", "wf_turnaround_cdf", 
             "wf_turnaround_stats", "wf_stretch_factor_cdf", 
             "wf_stretch_factor_stats", "wf_jobs_runtime_cdf",
             "wf_jobs_runtime_stats", "wf_jobs_cores_cdf", 
             "wf_jobs_cores_stats"]:
            assertEqualResult(self, old_results[field], 
                             new_results[field], field)
                    
        
    
    def test_calculate_job_results_per_manifest(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2",
                      "wf_manifest2-4_S0"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11,
                         12],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22,
                         30],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25,
                         35],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2,
                           3],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4,
                           33]}
        
        
        we.extract(job_list)
        we.do_processing()
        
        we.calculate_per_manifest_results(True, db_obj, 1)
        
        self.assertEqual(db_obj._id_count, 24)
        self.assertEqual(sorted(db_obj._set_fields),
            sorted(["m_manifest2_wf_runtime_cdf", 
             "m_manifest2_wf_runtime_stats", "m_manifest2_wf_waittime_cdf",
             "m_manifest2_wf_waittime_stats", "m_manifest2_wf_turnaround_cdf", 
             "m_manifest2_wf_turnaround_stats", "m_manifest2_wf_stretch_factor_cdf", 
             "m_manifest2_wf_stretch_factor_stats", "m_manifest2_wf_jobs_runtime_cdf",
             "m_manifest2_wf_jobs_runtime_stats", "m_manifest2_wf_jobs_cores_cdf", 
             "m_manifest2_wf_jobs_cores_stats",
             "m_manifest_wf_runtime_cdf", 
             "m_manifest_wf_runtime_stats", "m_manifest_wf_waittime_cdf",
             "m_manifest_wf_waittime_stats", "m_manifest_wf_turnaround_cdf", 
             "m_manifest_wf_turnaround_stats", "m_manifest_wf_stretch_factor_cdf", 
             "m_manifest_wf_stretch_factor_stats", "m_manifest_wf_jobs_runtime_cdf",
             "m_manifest_wf_jobs_runtime_stats", "m_manifest_wf_jobs_cores_cdf", 
             "m_manifest_wf_jobs_cores_stats"]))
        self.assertEqual(db_obj._hist_count, 12)
        self.assertEqual(db_obj._stats_count, 12)
    
    def test_fill_per_manifest_values(self):
        db_obj = FakeDBObj(self)
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2",
                      "wf_manifest2-4_S0"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11,
                         12],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         2, 15, 17, 22,
                         30],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25,
                         35],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2,
                           3],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4,
                           33]}
        
        
        we.extract(job_list)
        we.do_processing()
        we.fill_per_manifest_values()
        self.assertEqual(sorted(we._detected_manifests),
                        ["manifest", "manifest2"])
        
        self.assertEqual(we._manifests_values["manifest"]["wf_runtime"],
                          [69,23])
        self.assertEqual(we._manifests_values["manifest"]["wf_waittime"], 
                         [0,0])
        self.assertEqual(we._manifests_values["manifest"]["wf_turnaround"], 
                         [69,23])
        self.assertEqual(
                     len(we._manifests_values["manifest"]["wf_stretch_factor"]),
                         2)
        self.assertEqual(
                     len(we._manifests_values["manifest"]["wf_jobs_runtime"]),
                         11)
        self.assertEqual(
                     len(we._manifests_values["manifest"]["wf_jobs_cores"]), 
                         11)
        
        self.assertEqual(we._manifests_values["manifest2"]["wf_runtime"],
                          [5])
        self.assertEqual(we._manifests_values["manifest2"]["wf_waittime"], 
                         [27])
        self.assertEqual(we._manifests_values["manifest2"]["wf_turnaround"], 
                         [32])
        self.assertEqual(
                     len(we._manifests_values["manifest2"]["wf_stretch_factor"]),
                         1)
        self.assertEqual(
                     len(we._manifests_values["manifest2"]["wf_jobs_runtime"]),
                         1)
        self.assertEqual(
                     len(we._manifests_values["manifest2"]["wf_jobs_cores"]), 
                         1)
        we.extract(job_list)
        we.do_processing()
        we.fill_per_manifest_values(append=True)
       
        self.assertEqual(we._manifests_values["manifest"]["wf_runtime"],
                          [69,23,69,23])
        self.assertEqual(we._manifests_values["manifest"]["wf_waittime"], 
                         [0,0,0,0])
        self.assertEqual(we._manifests_values["manifest"]["wf_turnaround"], 
                         [69,23,69,23])
        self.assertEqual(
                     len(we._manifests_values["manifest"]["wf_stretch_factor"]),
                         4)
        self.assertEqual(
                     len(we._manifests_values["manifest"]["wf_jobs_runtime"]),
                         22)
        self.assertEqual(
                     len(we._manifests_values["manifest"]["wf_jobs_cores"]), 
                         22)
        
        self.assertEqual(we._manifests_values["manifest2"]["wf_runtime"],
                          [5,5])
        self.assertEqual(we._manifests_values["manifest2"]["wf_waittime"], 
                         [27,27])
        self.assertEqual(we._manifests_values["manifest2"]["wf_turnaround"], 
                         [32,32])
        self.assertEqual(
                     len(we._manifests_values["manifest2"]["wf_stretch_factor"]),
                         2)
        self.assertEqual(
                     len(we._manifests_values["manifest2"]["wf_jobs_runtime"]),
                         2)
        self.assertEqual(
                     len(we._manifests_values["manifest2"]["wf_jobs_cores"]), 
                         2)
     
    def test_load_job_results_per_manifest(self):
        db_obj = self._db
        hist = Histogram()
        stat = NumericStats()
        self.addCleanup(self._del_table,"histograms")
        self.addCleanup(self._del_table,"numericStats")
        hist.create_table(db_obj)
        stat.create_table(db_obj)
        
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2",
                      "wf_manifest2-4_S0"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11,
                         12],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22,
                         30],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25,
                         35],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2,
                           3],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4,
                           33]}
        
        
        we.extract(job_list)
        we.do_processing()
        
        old_results=we.calculate_per_manifest_results(True, db_obj, 1)
        
        new_we = WorkflowsExtractor()
        new_results=new_we.load_per_manifest_results(db_obj, 1)
        self.assertEqual(sorted(list(new_results.keys())),
                         sorted( ["manifest2", "manifest"]))
        for manifest in ["manifest2", "manifest"]:
            for field in ["wf_runtime_cdf", "wf_runtime_stats",
                 "wf_waittime_cdf",
                 "wf_waittime_stats", "wf_turnaround_cdf", 
                 "wf_turnaround_stats", "wf_stretch_factor_cdf", 
                 "wf_stretch_factor_stats", "wf_jobs_runtime_cdf",
                 "wf_jobs_runtime_stats", "wf_jobs_cores_cdf", 
                 "wf_jobs_cores_stats"]:
                field = "m_"+manifest+"_"+field
                assertEqualResult(self, old_results[manifest][field], 
                                  new_results[manifest][field], field)
    
    def test_get_waste_changes(self):
        
        we = WorkflowsExtractor()
        job_list={"job_name":["wf_manifest-2_S0", 
                      "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                      "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                      "wf_manifest-2_S5_dS4-dS1",
                      "wf_manifest-2_S6_dS0",
                      "sim_job",
                      "wf_manifest-3_S0", "wf_manifest-3_S1_dS0",
                      "wf_manifest-3_S2_dS0", "wf_manifest-3_S3_dS1-dS2",
                      "wf_manifestSim.json-4"
                      ],
          "id_job":     [ 0,  1,  2,  3,  4,  5,  6,
                         7,
                         8, 9, 10, 11,
                         12],
          "time_start": [ 1, 15, 17, 22, 27, 42, 12,
                         20,
                         1, 15, 17, 22,
                         30],
          "time_end":   [10, 20, 40, 25, 29, 50, 70,
                         30,
                         10, 20, 19, 25,
                         250],
          "time_submit": [ 1, 1, 1, 1, 1, 1, 1,
                          20,
                           2, 2, 2, 2,
                           3],
          "cpus_alloc": [  1, 2, 3, 4, 5, 6, 7,
                           1,
                           1, 2, 3, 4,
                           144]}
        
        
        we.extract(job_list)
        we.do_processing()
        stamps_list, wastedelta_list, acc_waste = we.get_waste_changes()
        
        self.assertEqual(stamps_list,[30, 150, 250])
        self.assertEqual(wastedelta_list,[32, -32, 0])
        self.assertEqual(acc_waste,120*32)
        
class TestWorkflowTracker(unittest.TestCase):
    
    def test_constructor(self):
        wt = WorkflowTracker("manifest")
        self.assertEqual(wt._name, "manifest")
    def test_register_task(self):
        job_list={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-2_S3_dS1-dS2" ],
                  "id_job":[1,2,3]}
        wt = WorkflowTracker("manifest")
        wt.register_task(job_list, 1)
        wt.register_task(job_list, 2)
        t = wt._tasks["S1"]
        self.assertEqual(t.name, "wf_manifest-2_S1_dS0")
        self.assertEqual(t.job_id, 2)
        self.assertEqual(t._parent_workflow, wt)
        self.assertEqual(t.stage_id, "S1")
        self.assertEqual(t.deps, ["S0"])
        t = wt._tasks["S3"]
        self.assertEqual(t.deps, ["S1", "S2"])
    
    def test_get_critical_path(self):
        job_list={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0",
                              "wf_manifest-2_S2_dS0", "wf_manifest-2_S3_dS1-dS2"],
                  "id_job":     [ 0,  1,  2,  3],
                  "time_start": [ 0, 15, 17, 22],
                  "time_end":   [10, 20, 19, 25]}
        wt = WorkflowTracker("manifest")
        for i in range(4):
            wt.register_task(job_list,i)
        wt.fill_deps()
        t0 = wt._tasks["S0"]
        t1 = wt._tasks["S1"]
        t2 = wt._tasks["S2"]
        t3 = wt._tasks["S3"]
        
        self.assertEqual(t0.dependenciesTo, [t1,t2])
        self.assertEqual(t1.dependenciesTo, [t3])
        self.assertEqual(t2.dependenciesTo, [t3])
        self.assertEqual(t3.dependenciesTo, [])
        
        self.assertEqual(wt._start_task,t0)
        self.assertEqual(wt._critical_path, [t0,t1,t3])
        self.assertEqual(wt._critical_path_runtime, 25)    
   
    def test_get_critical_path_complex(self):
        job_list={"job_name":["wf_manifest-2_S0", 
                              "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                              "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                              "wf_manifest-2_S5_dS4-dS1"
                              ],
                  "id_job":     [ 0,  1,  2,  3,  4,  5],
                  "time_start": [ 0, 15, 17, 22, 27, 42],
                  "time_end":   [10, 20, 40, 25, 29, 50]}
        wt = WorkflowTracker("manifest")
        for i in range(6):
            wt.register_task(job_list,i)
        wt.fill_deps()
        t0 = wt._tasks["S0"]
        t1 = wt._tasks["S1"]
        t2 = wt._tasks["S2"]
        t3 = wt._tasks["S3"]
        t4 = wt._tasks["S4"]
        t5 = wt._tasks["S5"]
        
        self.assertEqual(t0.dependenciesTo, [t1,t2])
        self.assertEqual(t1.dependenciesTo, [t5])
        self.assertEqual(t2.dependenciesTo, [t3])
        self.assertEqual(t3.dependenciesTo, [t4])
        self.assertEqual(t4.dependenciesTo, [t5])
        
        self.assertEqual(wt._start_task,t0)
        self.assertEqual(wt._critical_path, [t0,t1,t5])
        self.assertEqual(wt._critical_path_runtime, 50)  

    def test_get_critical_path_complex_more_endings(self):
        job_list={"job_name":["wf_manifest-2_S0", 
                              "wf_manifest-2_S1_dS0", "wf_manifest-2_S2_dS0", 
                              "wf_manifest-2_S3_dS2", "wf_manifest-2_S4_dS3",
                              "wf_manifest-2_S5_dS4-dS1",
                              "wf_manifest-2_S6_dS0"
                              ],
                  "id_job":     [ 0,  1,  2,  3,  4,  5,  6],
                  "time_start": [ 0, 15, 17, 22, 27, 42, 12],
                  "time_end":   [10, 20, 40, 25, 29, 50, 70]}
        wt = WorkflowTracker("manifest")
        for i in range(7):
            wt.register_task(job_list,i)
      
        t0 = wt._tasks["S0"]
        t1 = wt._tasks["S1"]
        t2 = wt._tasks["S2"]
        t3 = wt._tasks["S3"]
        t4 = wt._tasks["S4"]
        t5 = wt._tasks["S5"]
        t6 = wt._tasks["S6"]
        wt.fill_deps()
        
        self.assertEqual(t0.dependenciesTo, [t1,t2, t6])
        self.assertEqual(t1.dependenciesTo, [t5])
        self.assertEqual(t2.dependenciesTo, [t3])
        self.assertEqual(t3.dependenciesTo, [t4])
        self.assertEqual(t4.dependenciesTo, [t5])
        self.assertEqual(t5.dependenciesTo, [])
        self.assertEqual(t6.dependenciesTo, [])
        
        self.assertEqual(wt._start_task,t0)
        self.assertEqual(wt._critical_path, [t0,t6])
        self.assertEqual(wt._critical_path_runtime, 70)
    
    def test_get_waste_changes(self):
        job_list={"job_name":["wf_manifestSim.json"],
                  "id_job":     [ 2 ],
                  "time_start": [ 1000 ],
                  "time_end":   [1220 ]}
        wt = WorkflowTracker("manifestSim.json-2")
        for i in range(1):
            wt.register_task(job_list,i, True)
        wt.fill_deps()
        stamps, wastes, acc_waste = wt.get_waste_changes()
        
        self.assertEqual(stamps,[1000, 1120, 1220])
        self.assertEqual(wastes,[32, -32, 0])
        self.assertEqual(acc_waste, 32*120)
        

class TestTaskTracker(unittest.TestCase):
    def test_constructor(self):
        job_list={"job_name":["wf_manifest-2_S0", "wf_manifest-2_S1_dS0" ],
                  "id_job":[1,2]}
                              
        t = TaskTracker(job_list, 1, self)
        self.assertEqual(t.name, "wf_manifest-2_S1_dS0")
        self.assertEqual(t.job_id, 2)
        self.assertEqual(t._parent_workflow, self)
        self.assertEqual(t.stage_id, "S1")
        self.assertEqual(t.deps, ["S0"])
        
        self.assertEqual(t.dependenciesTo, [])
        t2=TaskTracker(job_list, 0, self)
        t.add_dep_to(t2)
        self.assertEqual(t.dependenciesTo, [t2])
    
class TestWasteExtractor(unittest.TestCase):   
    def test_constructor(self):
        os.environ["MANIFEST_DIR"] = "dir/dir"
        we = WasteExtractor("Text")
        self.assertEqual(we._manifest, "dir/dir/Text")
        del os.environ["MANIFEST_DIR"]

    def test_get_waste_changes(self):
        we = WasteExtractor("./manifest_sim.json")
        time_stamps, wastes, acc = we.get_waste_changes(3000)
        self.assertEqual(time_stamps,[3000, 3120, 3220])
        self.assertEqual(wastes,[32, -32, 0])
        self.assertEqual(acc, 120*32)
    def test_fuse_waste_changes(self):
        stamps_list, usage_list  = _fuse_delta_lists([100, 200, 300],
                                                       [20, 20, -40],
                                                       [200, 301],
                                                       [10, -10])
        self.assertEqual(stamps_list, [100, 200, 300, 301])
        self.assertEqual(usage_list, [20, 30, -40, -10])
        
        stamps_list, usage_list  = _fuse_delta_lists([100, 200, 300],
                                                       [20, 20, -40],
                                                       [50, 301],
                                                       [10, -10])
        self.assertEqual(stamps_list, [50, 100, 200, 300, 301])
        self.assertEqual(usage_list, [10, 20, 20, -40, -10])
        
class Test_apply_deltas_usage(unittest.TestCase):
    def test_apply_deltas_usage(self):
        stamps_list, usage_list  = _apply_deltas_usage([100, 200, 300],
                                                       [20, 20, 40],
                                                       [200, 301],
                                                       [10, -10], neg=True)
        self.assertEqual(stamps_list, [100, 200, 300, 301])
        self.assertEqual(usage_list,  [20,  10,  30, 40 ])
        
        stamps_list, usage_list  = _apply_deltas_usage([100, 200, 300],
                                                       [20, 20, 40],
                                                       [50, 301],
                                                       [10, -10], neg=True)
        self.assertEqual(stamps_list, [50, 100, 200, 300, 301])
        self.assertEqual(usage_list,  [-10, 10,  10,  30, 40 ])
        
       