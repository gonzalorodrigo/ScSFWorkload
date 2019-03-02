
"""UNIT TESTS 

 python -m unittest test_orchestration
 
 
"""

import datetime
import os
import unittest

from commonLib.DBManager import DB
from commonLib.nerscUtilization import UtilizationEngine
from orchestration import AnalysisWorker
from orchestration import ExperimentWorker
from orchestration import get_central_db, get_sim_db
from orchestration.analyzing import AnalysisRunnerSingle
from orchestration.definition import (ExperimentDefinition,
                                       DeltaExperimentDefinition,
                                       GroupExperimentDefinition)
from orchestration.definition import ExperimentDefinition
from orchestration.running import ExperimentRunner
from stats import Histogram, NumericStats, NumericList
from stats.compare import WorkflowDeltas
from stats.trace import ResultTrace


class TestOrchestration(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
        self._vm_ip = os.getenv("TEST_VM_HOST", "192.168.56.24")
        
        ExperimentDefinition().create_table(self._db)
        self.addCleanup(self._del_table, ExperimentDefinition()._table_name)
        
        ht = Histogram()
        ht.create_table(self._db)
        self.addCleanup(self._del_table, ht._table_name)
        
        ns = NumericStats()
        ns.create_table(self._db)
        self.addCleanup(self._del_table, ns._table_name)
        
        us = NumericList("usage_values", ["utilization", "waste"
                                          "corrected_utilization"])
        us.create_table(self._db)
        self.addCleanup(self._del_table, "usage_values") 
        
        rt = ResultTrace()
       
        self.addCleanup(self._del_table,"traces" )
        rt.create_trace_table(self._db, "traces")
        ExperimentRunner.configure(
           trace_folder="/tmp/",
           trace_generation_folder="tmp", 
           local=False,
           run_hostname=self._vm_ip,
           run_user=None,
           scheduler_conf_dir="/scsf/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/scsf/",
           manifest_folder="manifests",
           drain_time=0)
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
    def _del_exp(self, exp_def, db_obj):
        exp_def.del_results(db_obj)
        exp_def.del_trace(db_obj)
        exp_def.del_exp(db_obj)
    
    def test_single_no_wf_create_sim_analysis(self):
        db_obj = self._db
        exp = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[],
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_handling="single",
                 preload_time_s = 0,
                 workload_duration_s=3600*1)
        self.addCleanup(self._del_exp, exp, db_obj)
        exp.store(db_obj)
        
        sched_db_obj = get_sim_db(self._vm_ip)
        
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        
        self._check_trace_is_there(db_obj,exp)
        
        ew = AnalysisWorker()

        ew.do_work_single(db_obj)

        self._check_results_are_there(db_obj, exp)
        
    def test_single_with_wf_create_sim_analysis(self):
        db_obj = self._db
        exp = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="single",
                 preload_time_s = 0,
                 workload_duration_s=3600*1)
        self.addCleanup(self._del_exp, exp, db_obj)
        exp.store(db_obj)
        
        sched_db_obj = get_sim_db(self._vm_ip)
        
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        
        self._check_trace_is_there(db_obj,exp)
        
        ew = AnalysisWorker()

        ew.do_work_single(db_obj)

        self._check_results_are_there(db_obj, exp, True, 
                                      ["manifestsim.json"])
    
    def test_multi_with_wf_create_sim_analysis(self):
        db_obj = self._db
        exp = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="multi",
                 preload_time_s = 0,
                 workload_duration_s=3600*1)
        self.addCleanup(self._del_exp, exp, db_obj)
        exp.store(db_obj)
        
        sched_db_obj = get_sim_db(self._vm_ip)
        
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        
        self._check_trace_is_there(db_obj,exp)
        
        ew = AnalysisWorker()

        ew.do_work_single(db_obj)

        self._check_results_are_there(db_obj, exp, True, 
                                      ["manifestsim.json"])
    def test_manifest_with_wf_create_sim_analysis(self):
        db_obj = self._db
        exp = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="manifest",
                 preload_time_s = 0,
                 workload_duration_s=3600*1)
        self.addCleanup(self._del_exp, exp, db_obj)
        exp.store(db_obj)
        
        sched_db_obj = get_sim_db(self._vm_ip)
        
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        
        self._check_trace_is_there(db_obj,exp)
        
        ew = AnalysisWorker()

        ew.do_work_single(db_obj)

        self._check_results_are_there(db_obj, exp, True, 
                                      ["manifestsim.json"])
    
    def _check_trace_is_there(self, db_obj, exp): 
        trace_id=exp._trace_id
        new_ew = ExperimentDefinition()
        new_ew.load(db_obj, trace_id)
        self.assertEqual(new_ew._work_state, "simulation_done")

        result_trace = ResultTrace()
        result_trace.load_trace(db_obj, trace_id)
        self.assertTrue(result_trace._lists_submit)
        
        self.assertLessEqual(result_trace._lists_submit["time_submit"][0], 
                                exp.get_start_epoch()-exp._preload_time_s+60)
        self.assertGreaterEqual(result_trace._lists_submit["time_submit"][-1], 
                                exp.get_end_epoch()-60)
    def _check_results_are_there(self, db_obj, exp, wf=False, manifest_list=[],
                                 job_fields=None):
        if job_fields is None:
            job_fields=["jobs_runtime_cdf", "jobs_runtime_stats",
             "jobs_waittime_cdf",
             "jobs_waittime_stats", "jobs_turnaround_cdf", 
             "jobs_turnaround_stats", "jobs_requested_wc_cdf", 
             "jobs_requested_wc_stats", "jobs_cpus_alloc_cdf",
             "jobs_cpus_alloc_stats"]
        trace_id=exp._trace_id
        new_ew = ExperimentDefinition()
        new_ew.load(db_obj, trace_id)
        self.assertEqual(new_ew._work_state, "analysis_done")
        result_trace = ResultTrace()
        result_trace.load_analysis(db_obj, trace_id)
        print "KK", result_trace.jobs_results
        print "I AN HERE"
        
        for field in job_fields:
            self.assertNotEqual(result_trace.jobs_results[field], None)
        if (wf):
            results=result_trace.workflow_results
            results_per_wf = result_trace.workflow_results_per_manifest
            print results
            print results_per_wf
            for field in ["wf_runtime_cdf", "wf_runtime_stats",
             "wf_waittime_cdf",
             "wf_waittime_stats", "wf_turnaround_cdf", 
             "wf_turnaround_stats", "wf_stretch_factor_cdf", 
             "wf_stretch_factor_stats", "wf_jobs_runtime_cdf",
             "wf_jobs_runtime_stats", "wf_jobs_cores_cdf", 
             "wf_jobs_cores_stats"]:
                self.assertNotEqual(results[field], None)
                for manifest in manifest_list:
                    this_manifest_result=results_per_wf[manifest]
                    self.assertNotEqual(
                            this_manifest_result["m_"+manifest+"_"+field], None)
    
    
    def test_delta_exp(self):
        exp1 = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="multi",
                 preload_time_s = 0,
                 workload_duration_s=600*1)
        id1=exp1.store(self._db)
        exp2 = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="multi",
                 preload_time_s = 0,
                 workload_duration_s=600*1)
        id2=exp2.store(self._db)
        exp3 = DeltaExperimentDefinition(subtraces=[id1, id2])
        
        id3=exp3.store(self._db)
        sched_db_obj = get_sim_db(self._vm_ip)
        ew = ExperimentWorker()
        ew.do_work(self._db, sched_db_obj)

        
        ew = AnalysisWorker()

        ew.do_work_delta(self._db)
        trace_comparer = WorkflowDeltas()
        results=trace_comparer.load_delta_results(self._db, exp3._trace_id)
        for result_name in ["delta_runtime_cdf", "delta_waittime_cdf", 
                        "delta_turnaround_cdf",
                        "delta_stretch_cdf",
                        "delta_runtime_stats", "delta_waittime_stats",
                        "delta_turnaround_stats",
                        "delta_stretch_stats"]:
            self.assertIn(result_name, results.keys())
            if "_cdf" in result_name:
                self.assertIs(type(results[result_name]), Histogram)
            elif "_stats" in result_name:
                self.assertIs(type(results[result_name]), NumericStats)
           
    def test_grouped_exp(self):
        exp1 = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="multi",
                 preload_time_s = 0,
                 workload_duration_s=600*1)
        id1=exp1.store(self._db)
        exp2 = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share":1.0, 
                                 "manifest":"manifestsim.json"}],
                 workflow_policy="period",
                 workflow_period_s=60,
                 workflow_handling="multi",
                 preload_time_s = 0,
                 workload_duration_s=600*1)
        
        id2=exp2.store(self._db)
        exp3 = GroupExperimentDefinition(subtraces=[id1, id2])
        
        id3=exp3.store(self._db)
        sched_db_obj = get_sim_db(self._vm_ip)
        ew = ExperimentWorker()
        ew.do_work(self._db, sched_db_obj)
        
        aw = AnalysisWorker()
        aw.do_work_single(self._db)

        
        aw.do_work_grouped(self._db)
        self._check_results_are_there(self._db, exp3, wf=True, 
                                      manifest_list=["manifestsim.json"])   
            