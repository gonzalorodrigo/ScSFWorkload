#         self._fields="""`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
#           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
#           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
#           `time_submit`, `time_start`, `time_end`"""

"""UNIT TESTS for the the class storing traces

 python -m unittest test_ResultTrace
 
 
"""

from commonLib.DBManager import DB
from stats.trace import ResultTrace
from stats import Histogram, NumericStats

import numpy as np
import os
import unittest

class TestResultTrace(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
    def test_join_dics_of_lists(self):
        dict1={"key1": [1, 2, 3], "key2":[4,5,6]}
        dict2={"key2": [7,8,9], "key3": [10,11,12] }
        new_dict = ResultTrace.join_dics_of_lists(dict1, dict2)
        
        self.assertDictEqual(new_dict, 
                            {"key1": [1, 2, 3], 
                             "key2":[4,5,6, 7, 8 ,9],
                             "key3": [10,11,12]
                             })
        
        
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table `"+table_name+"`")
        self.assertTrue(ok, "Table was not created!")
    def _create_tables(self):
        rt = ResultTrace()
        self.addCleanup(self._del_table,"import_table" )
        rt.create_import_table(self._db, "import_table")
        self.addCleanup(self._del_table,"traces" )
        rt.create_trace_table(self._db, "traces")
    
    def test_create_tables(self):
        self._create_tables()
        rows = self._db.doQuery("show tables")
        self.assertIn(("import_table",), rows)
        self.assertIn(("traces",), rows)
        
    def test_import_from_db(self):
        self._create_tables()
        self._db.doUpdate(
        """insert into  import_table 
          (`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
           `time_submit`, `time_start`, `time_end`) VALUES (
           1, "account1", 48, 48,
           "jobName1", 1, 2, 3, 4, 
           2, "partition1", 99, 3, 100,
           3000, 3002, 3002  
           )""")
        
        self._db.doUpdate(
        """insert into  import_table 
          (`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
           `time_submit`, `time_start`, `time_end`) VALUES (
           2, "account2", 96, 96,
           "jobName2", 2, 3, 4, 5, 
           4, "partition2", 199, 2, 200,
           3003, 3001, 3005  
           )""")
        
        rt = ResultTrace()
        rt.import_from_db(self._db, "import_table")        
        compare_data= {
             "job_db_inx":[1,2],
             "account": ["account1", "account2"],
             "cpus_req": [48, 96],
             "cpus_alloc": [48, 96],
             "job_name":["jobName1", "jobName2"],
             "id_job": [1,2],
             "id_qos": [2,3],
             "id_resv": [3,4],
             "id_user": [4,5],
             "nodes_alloc": [2,4],
             "partition": ["partition1", "partition2"],
             "priority": [99, 199],
             "state": [3,2],
             "timelimit": [100,200],
             "time_submit": [3000,3003],
             "time_start": [3002,3001],
             "time_end": [3002,3005]                      
             }
        for key in compare_data.keys():
            self.assertEqual(compare_data[key], rt._lists_submit[key])
        
        compare_data= {
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
        for key in compare_data.keys():
            self.assertEqual(compare_data[key], rt._lists_start[key])
          
    def test_clean_dumplicates_db(self):
        self._create_tables()
        self._db.doUpdate(
        """insert into  import_table 
          (`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
           `time_submit`, `time_start`, `time_end`) VALUES (
           0, "account1", 48, 48,
           "jobName1", 1, 2, 3, 4, 
           2, "partition1", 99, 3, 100,
           3000, 3002, 3002  
           )""")
        
        self._db.doUpdate(
        """insert into  import_table 
          (`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
           `time_submit`, `time_start`, `time_end`) VALUES (
           1, "account1", 48, 48,
           "jobName1", 1, 2, 3, 4, 
           2, "partition1", 99, 3, 100,
           3000, 3002, 3002  
           )""")
        self._db.doUpdate(
        """insert into  import_table 
          (`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
           `time_submit`, `time_start`, `time_end`) VALUES (
           2, "account2", 96, 96,
           "jobName2", 2, 3, 4, 5, 
           4, "partition2", 199, 2, 200,
           2003, 2001, 2005  
           )""")
        
                
        self._db.doUpdate(
        """insert into  import_table 
          (`job_db_inx`, `account`, `cpus_req`, `cpus_alloc`,
           `job_name`, `id_job`, `id_qos`, `id_resv`, `id_user`, 
           `nodes_alloc`, `partition`, `priority`, `state`, `timelimit`,
           `time_submit`, `time_start`, `time_end`) VALUES (
           3, "account2", 96, 96,
           "jobName2", 2, 3, 4, 5, 
           4, "partition2", 199, 2, 200,
           3003, 3001, 3005  
           )""")
        
        rt = ResultTrace()
        rt.import_from_db(self._db, "import_table")     
        print  rt._lists_submit   
        compare_data= {
             "job_db_inx":[1,3],
             "account": ["account1", "account2"],
             "cpus_req": [48, 96],
             "cpus_alloc": [48, 96],
             "job_name":["jobName1", "jobName2"],
             "id_job": [1,2],
             "id_qos": [2,3],
             "id_resv": [3,4],
             "id_user": [4,5],
             "nodes_alloc": [2,4],
             "partition": ["partition1", "partition2"],
             "priority": [99, 199],
             "state": [3,2],
             "timelimit": [100,200],
             "time_submit": [3000,3003],
             "time_start": [3002,3001],
             "time_end": [3002,3005]                      
             }
        for key in compare_data.keys():
            self.assertEqual(compare_data[key], rt._lists_submit[key])
        
        compare_data= {
             "job_db_inx":[3,1],
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
        for key in compare_data.keys():
            self.assertEqual(compare_data[key], rt._lists_start[key])
            
    def test_store_trace(self):
        self._create_tables()
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2],
             "account": ["account1", "account2"],
             "cpus_req": [48, 96],
             "cpus_alloc": [48, 96],
             "job_name":["jobName1", "jobName2"],
             "id_job": [1,2],
             "id_qos": [2,3],
             "id_resv": [3,4],
             "id_user": [4,5],
             "nodes_alloc": [2,4],
             "partition": ["partition1", "partition2"],
             "priority": [99, 199],
             "state": [3,2],
             "timelimit": [100,200],
             "time_submit": [3000,3003],
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
        
        rows = self._db.doQuery("SELECT time_start FROM traces "
                            "WHERE trace_id=1 "
                            "ORDER BY time_start")
        self.assertIn((3001,), rows)
        self.assertIn((3002,), rows)
    def test_store_load_trace(self):
        self._create_tables()
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2],
             "account": ["account1", "account2"],
             "cpus_req": [48, 96],
             "cpus_alloc": [48, 96],
             "job_name":["jobName1", "jobName2"],
             "id_job": [1,2],
             "id_qos": [2,3],
             "id_resv": [3,4],
             "id_user": [4,5],
             "nodes_alloc": [2,4],
             "partition": ["partition1", "partition2"],
             "priority": [99, 199],
             "state": [3,2],
             "timelimit": [100,200],
             "time_submit": [3000,3003],
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
        new_rt = ResultTrace()
        new_rt.load_trace(self._db,1)
        self.assertEqual(rt._lists_start, new_rt._lists_start)
        self.assertEqual(rt._lists_submit, new_rt._lists_submit)
        
    def test_multi_load_trace(self):
        self._create_tables()
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2],
             "account": ["account1", "account2"],
             "cpus_req": [48, 96],
             "cpus_alloc": [48, 96],
             "job_name":["jobName1", "jobName2"],
             "id_job": [1,2],
             "id_qos": [2,3],
             "id_resv": [3,4],
             "id_user": [4,5],
             "nodes_alloc": [2,4],
             "partition": ["partition1", "partition2"],
             "priority": [99, 199],
             "state": [3,2],
             "timelimit": [100,200],
             "time_submit": [3000,3003],
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
        new_rt = ResultTrace()
        new_rt.load_trace(self._db,1)
        new_rt.load_trace(self._db,1, True)
        self.assertEqual(new_rt._lists_submit["time_submit"],
                         [3000, 3003, 3004, 3007])
        self.assertEqual(new_rt._lists_submit["time_start"],
                         [3002, 3001, 3006, 3005])
        self.assertEqual(new_rt._lists_submit["time_end"],
                         [3002, 3005, 3006, 3009])
        
        self.assertEqual(new_rt._lists_start["time_start"],
                         [3001, 3002, 3005, 3006])
        self.assertEqual(new_rt._lists_start["time_submit"],
                         [3003, 3000, 3007, 3004])
        self.assertEqual(new_rt._lists_start["time_end"],
                         [3005, 3002, 3009, 3006])
        
    def test_multi_load_results(self):
        self._create_tables()
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "account1"],
             "cpus_req": [48, 96, 24],
             "cpus_alloc": [48, 96, 24],
             "job_name":["jobName1", "jobName2", "wf_manifest"],
             "id_job": [1,2,3],
             "id_qos": [2,3,4],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,1],
             "partition": ["partition1", "partition2", "partition1"],
             "priority": [99, 199, 99],
             "state": [3,2, 3],
             "timelimit": [100,200, 200],
             "time_submit": [3000,3003, 3500],
             "time_start": [3002,3004, 3501],
             "time_end": [3003,3005, 3510]                      
             }
        rt._lists_start = {
             "job_db_inx":[2,1,3],
             "account": ["account2","account1", "account1"],
             "cpus_req": [96, 48, 24],
             "cpus_alloc": [96, 48, 24],
             "job_name":["jobName2", "jobName1", "wf_manifest"],
             "id_job": [2,1,3],
             "id_qos": [3,2, 4],
             "id_resv": [4,3,5],
             "id_user": [5,4, 6],
             "nodes_alloc": [4,2,1],
             "partition": ["partition2","partition1", "partition1"],
             "priority": [199, 99, 99],
             "state": [2,3, 3],
             "timelimit": [200,100, 200],
             "time_submit": [3003,3000, 3500],
             "time_start": [3004,3002, 3501],
             "time_end": [3005,3002, 3510]                      
             }
        rt.store_trace(self._db, 1)
        new_rt = ResultTrace()
        new_rt.load_trace(self._db,1)
        new_rt.fill_job_values(start=3000, stop=4000)
        new_rt.load_trace(self._db,1)
        new_rt.fill_job_values(start=3000, stop=4000, append=True)
        
        self.assertEqual(new_rt._jobs_runtime, [1,1,1,1])
        self.assertEqual(new_rt._jobs_waittime, [2,1,2,1])
        self.assertEqual(new_rt._jobs_turnaround, [3,2,3,2])
        self.assertEqual(new_rt._jobs_timelimit, [100,200,100,200])
        self.assertEqual(new_rt._jobs_cpus_alloc, [48,96,48,96])     
        self.assertEqual(new_rt._jobs_slowdown, [3,2,3,2])     
    
    def test_get_job_times(self):
        rt = ResultTrace()
        rt._lists_submit["time_end"] = [10, 10, 10000, 55, 330]
        rt._lists_submit["time_start"] = [5, 2, 1000, 50, 290]
        rt._lists_submit["time_submit"] = [0, 2, 30, 100, 200]
        rt._lists_submit["job_name"] = ["J0", "J1", "J2", "J3", "wf_man"]
        rt._lists_submit["timelimit"] = [1, 2, 3, 4, 5]
        rt._lists_submit["cpus_alloc"] = [10, 20, 30, 40, 50]
        
        (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
          jobs_cores_alloc, jobs_slow_down)= rt._get_job_times(only_non_wf=True)
        self.assertEqual(jobs_runtime, [8, 9000])
        self.assertEqual(jobs_waittime, [0, 970])
        self.assertEqual(jobs_turnaround, [8, 9970])
        self.assertEqual(jobs_timelimit, [2,3])
        self.assertEqual(jobs_cores_alloc, [20,30])
        self.assertEqual(jobs_slow_down, [1.0, 9970.0/9000.0])
        
    def test_get_job_times_limits(self):
        rt = ResultTrace()
        rt._lists_submit["time_end"] = [10, 10, 10000, 140]
        rt._lists_submit["time_start"] = [5, 2, 1000, 120]
        rt._lists_submit["time_submit"] = [0, 2, 30, 100]
        rt._lists_submit["job_name"] = ["J0", "J1", "J2", "J3"]
        rt._lists_submit["timelimit"] = [1, 2, 3, 4]
        rt._lists_submit["cpus_alloc"] = [10, 20, 30, 40]
        
         
        (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
          jobs_cores_alloc, jobs_slow_down) = rt._get_job_times(submit_start=20,
                                                submit_stop=40)
        self.assertEqual(jobs_runtime, [9000])
        self.assertEqual(jobs_waittime, [970])
        self.assertEqual(jobs_turnaround, [9970])
        self.assertEqual(jobs_timelimit, [3])
        self.assertEqual(jobs_cores_alloc, [30])
        self.assertEqual(jobs_slow_down, [9970.0/9000.0])
        
    def test_get_job_times_grouped(self):
        rt = ResultTrace()
        rt._lists_submit["time_end"] = [10, 10, 10000, 55, 330, 460]
        rt._lists_submit["time_start"] = [5, 2, 1000, 50, 290, 400]
        rt._lists_submit["time_submit"] = [0, 2, 30, 100, 200, 300]
        rt._lists_submit["job_name"] = ["J0", "J1", "J2", "J3", "wf_man", "J4"]
        rt._lists_submit["timelimit"] = [1, 2, 3, 4, 5, 3]
        rt._lists_submit["cpus_alloc"] = [1, 1, 30, 40, 50, 4]
        
        cores_seconds_edges=[0, 500, 1000]
        
        (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
          jobs_cores_alloc, jobs_slow_down, jobs_timesubmit)= (
                                        rt.get_job_times_grouped_core_seconds(
                                                    cores_seconds_edges,
                                                    only_non_wf=True,
                                                    submit_start=0,
                                                    submit_stop=10000000))
        self.assertEqual(jobs_runtime[0], [8])
        self.assertEqual(jobs_waittime[0], [0])
        self.assertEqual(jobs_turnaround[0], [8])
        self.assertEqual(jobs_timelimit[0], [2])
        self.assertEqual(jobs_cores_alloc[0], [1])
        self.assertEqual(jobs_slow_down[0], [1])
        self.assertEqual(jobs_timesubmit[0], [2])
        
        self.assertEqual(jobs_runtime[500], [60])
        self.assertEqual(jobs_waittime[500], [100])
        self.assertEqual(jobs_turnaround[500], [160])
        self.assertEqual(jobs_timelimit[500], [3])
        self.assertEqual(jobs_cores_alloc[500], [4])
        self.assertEqual(jobs_slow_down[500], [160.0/60.0])
        self.assertEqual(jobs_timesubmit[500], [300])
        
        self.assertEqual(jobs_runtime[1000], [9000])
        self.assertEqual(jobs_waittime[1000], [970])
        self.assertEqual(jobs_turnaround[1000], [9970])
        self.assertEqual(jobs_timelimit[1000], [3])
        self.assertEqual(jobs_cores_alloc[1000], [30])
        self.assertEqual(jobs_slow_down[1000], [9970.0/9000])
        self.assertEqual(jobs_timesubmit[1000], [30])
    
    
    def test_transform_pbs_to_slurm(self):        
        pbs_list = {"account": ["account1", "account2"],
                    "cores_per_node": [24, 48],
                    "numnodes" : [100, 200],
                    "class": ["queue1", "queue2"],
                    "wallclock_requested": [120, 368],
                    "created": [1000, 2000],
                    "start": [1100, 2200],
                    "completion": [1500, 2700],
                    "jobname": ["name1", "name2"]
                    } 
        rt = ResultTrace()
        slurm_list = rt._transform_pbs_to_slurm(pbs_list)
        
        self.assertEqual(slurm_list["job_db_inx"],
                         [0,1])
        self.assertEqual(slurm_list["account"],
                         ["account1", "account2"])
        self.assertEqual(slurm_list["cpus_req"],
                         [2400, 9600])
        self.assertEqual(slurm_list["cpus_alloc"],
                         [2400, 9600])
        self.assertEqual(slurm_list["job_name"],
                         ["name1", "name2"])
        self.assertEqual(slurm_list["id_job"],
                         [0,1])
        self.assertEqual(slurm_list["id_qos"],
                         [3,3])
        self.assertEqual(slurm_list["id_resv"],
                         [3,3])
        self.assertEqual(slurm_list["id_user"],
                         [3,3])
        self.assertEqual(slurm_list["nodes_alloc"],
                         [100,200])
        self.assertEqual(slurm_list["partition"],
                         ["queue1","queue2"])
        self.assertEqual(slurm_list["priority"],
                         [3,3])
        self.assertEqual(slurm_list["state"],
                         [3,3])
        self.assertEqual(slurm_list["timelimit"],
                         [2,6])
        self.assertEqual(slurm_list["time_submit"],
                         [1000,2000])
        self.assertEqual(slurm_list["time_start"],
                         [1100,2200])
        self.assertEqual(slurm_list["time_end"],
                         [1500,2700])
    def test_calculate_job_results(self):
        db_obj = FakeDBObj(self)
        rt = ResultTrace()
        pbs_list = {"account": ["account1", "account2"],
            "cores_per_node": [24, 48],
            "numnodes" : [100, 200],
            "class": ["queue1", "queue2"],
            "wallclock_requested": [120, 368],
            "created": [1000, 2000],
            "start": [1100, 2200],
            "completion": [1500, 2700],
            "jobname": ["name1", "name2"]
            } 
        rt._lists_submit = rt._transform_pbs_to_slurm(pbs_list)
        
        rt.calculate_job_results(True, db_obj, 1)
        
        self.assertEqual(db_obj._id_count, 12)
        self.assertEqual(db_obj._set_fields,
            ["jobs_runtime_cdf", "jobs_runtime_stats", "jobs_waittime_cdf",
             "jobs_waittime_stats", "jobs_turnaround_cdf", 
             "jobs_turnaround_stats", "jobs_requested_wc_cdf", 
             "jobs_requested_wc_stats", "jobs_cpus_alloc_cdf",
             "jobs_cpus_alloc_stats",
             "jobs_slowdown_cdf", "jobs_slowdown_stats"])
        self.assertEqual(db_obj._hist_count, 6)
        self.assertEqual(db_obj._stats_count, 6)

    def test_load_job_results(self):
        db_obj = self._db
        hist = Histogram()
        stat = NumericStats()
        self.addCleanup(self._del_table,"histograms")
        self.addCleanup(self._del_table,"numericStats")
        hist.create_table(db_obj)
        stat.create_table(db_obj)
        
        rt = ResultTrace()
        pbs_list = {"account": ["account1", "account2"],
            "cores_per_node": [24, 48],
            "numnodes" : [100, 200],
            "class": ["queue1", "queue2"],
            "wallclock_requested": [120, 368],
            "created": [1000, 2000],
            "start": [1100, 2200],
            "completion": [1500, 2700],
            "jobname": ["name1", "name2"]
            } 
        rt._lists_submit = rt._transform_pbs_to_slurm(pbs_list)
        
        rt.calculate_job_results(True, db_obj, 1)
        
        db_obj = self._db
        new_rt  = ResultTrace()
        new_rt.load_job_results(db_obj, 1)
        
        
        
        for field in ["jobs_runtime_cdf", "jobs_runtime_stats",
             "jobs_waittime_cdf",
             "jobs_waittime_stats", "jobs_turnaround_cdf", 
             "jobs_turnaround_stats", "jobs_requested_wc_cdf", 
             "jobs_requested_wc_stats", "jobs_cpus_alloc_cdf",
             "jobs_cpus_alloc_stats",
             "jobs_slowdown_cdf", "jobs_slowdown_stats"]:
            self.assertNotEqual(rt.jobs_results[field], None)
    
    def test_calculate_job_results_grouped_core_seconds(self):
        db_obj = FakeDBObj(self)
        rt = ResultTrace()
        pbs_list = {"account": ["account1", "account2"],
            "cores_per_node": [24, 24, 24],
            "numnodes" : [1, 1, 1],
            "wallclock_requested": [360, 500, 600],
            "class": ["queue1", "queue2", "queue3"],
            "created": [1000, 2000, 3000],
            "start": [1100, 2200,3300],
            "completion": [1500, 2700, 4000],
            "jobname": ["sim_job", "sim_job","sim_job"]
            } 
        rt._lists_submit = rt._transform_pbs_to_slurm(pbs_list)
        
        rt.calculate_job_results_grouped_core_seconds(
                                                [0, 24*450, 24*550],
                                                True, db_obj, 1)
        
        self.assertEqual(db_obj._id_count, 12*3)
        fields=["jobs_runtime_cdf", "jobs_runtime_stats", "jobs_waittime_cdf",
             "jobs_waittime_stats", "jobs_turnaround_cdf", 
             "jobs_turnaround_stats", "jobs_requested_wc_cdf", 
             "jobs_requested_wc_stats", "jobs_cpus_alloc_cdf",
             "jobs_cpus_alloc_stats",
             "jobs_slowdown_cdf", "jobs_slowdown_stats"]
        new_fields=[]
        for edge in[0, 24*450, 24*550]:
            for field in fields:
                new_fields.append("g"+str(edge)+"_"+field)
        self.assertEqual(db_obj._set_fields,
            new_fields)
        self.assertEqual(db_obj._hist_count, 6*3)
        self.assertEqual(db_obj._stats_count, 6*3)

    def test_load_job_results_grouped_core_seconds(self):
        db_obj = self._db
        hist = Histogram()
        stat = NumericStats()
        self.addCleanup(self._del_table,"histograms")
        self.addCleanup(self._del_table,"numericStats")
        hist.create_table(db_obj)
        stat.create_table(db_obj)
        
        rt = ResultTrace()
        pbs_list = {"account": ["account1", "account2"],
            "cores_per_node": [24, 24, 24],
            "numnodes" : [1, 1, 1],
            "wallclock_requested": [120, 368, 400],
            "class": ["queue1", "queue2", "queue3"],
            "created": [1000, 2000, 3000],
            "start": [1100, 2200,3300],
            "completion": [1500, 2700, 4000],
            "jobname": ["name1", "name2","name3"]
            } 
        rt._lists_submit = rt._transform_pbs_to_slurm(pbs_list)
        
        rt.calculate_job_results_grouped_core_seconds(
                                                [0, 24*450, 24*550],
                                                True, db_obj, 1)
        
        db_obj = self._db
        new_rt  = ResultTrace()
        new_rt.load_job_results_grouped_core_seconds(
                                                     [0, 24*450, 24*550],
                                                     db_obj, 1)
        
        fields=["jobs_runtime_cdf", "jobs_runtime_stats", "jobs_waittime_cdf",
             "jobs_waittime_stats", "jobs_turnaround_cdf", 
             "jobs_turnaround_stats", "jobs_requested_wc_cdf", 
             "jobs_requested_wc_stats", "jobs_cpus_alloc_cdf",
             "jobs_cpus_alloc_stats", 
             "jobs_slowdown_cdf", "jobs_slowdown_stats"]
        new_fields=[]
        for edge in [0, 24*450, 24*550]:
            for field in fields:
                new_fields.append("g"+str(edge)+"_"+field)
        
        for field in new_fields:
            self.assertNotEqual(new_rt.jobs_results[field], None)
    
    def test_utilization(self):
        rt = ResultTrace()
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
             "time_end": [3005,3010]                      
             }
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
             rt.calculate_utilization(144))
        
        self.assertEqual(utilization_timestamps, [3001, 3002, 3005, 3010])
        self.assertEqual(utilization_values,     [96,   144,  48,   0])
        self.assertEqual(acc_waste,0)
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
             rt.calculate_utilization(144, endCut=3006))
        self.assertEqual(utilization_timestamps, [3001, 3002, 3005, 3006])
        self.assertEqual(utilization_values,     [96,   144, 48, 48])
        self.assertEqual(acc_waste,0)
        
    def test_pre_load_utilization(self):
        rt = ResultTrace()
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
             "time_submit": [3000,3000],
             "time_start": [3001,3003],
             "time_end": [3005,3010]                      
             }
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
             rt.calculate_utilization(144, do_preload_until=3002))
        
        self.assertEqual(utilization_timestamps, [3002, 3003, 3005, 3010])
        self.assertEqual(utilization_values,     [96, 144,  48,   0])
        self.assertEqual(acc_waste, 0)
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
             rt.calculate_utilization(144, do_preload_until=3003, 
                                      endCut=3006))
        
        self.assertEqual(utilization_timestamps, [3003, 3005, 3006])
        self.assertEqual(utilization_values,     [144,  48, 48])
        self.assertEqual(acc_waste, 0)
        
        self.assertEqual(integrated_ut, (2.0*144.0+48.0)/(3.0*144.0))
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
        rt.calculate_utilization(144, do_preload_until=3003, 
                                      endCut=3005))
        
        self.assertEqual(utilization_timestamps, [3003, 3005])
        self.assertEqual(utilization_values,     [144,  48])
        self.assertEqual(integrated_ut, 1.0)
        self.assertEqual(acc_waste, 0)
    
    def test_utlization_waste(self):
        rt = ResultTrace()
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
             "time_submit": [3000,3000],
             "time_start": [3001,3003],
             "time_end": [3005,3010]                      
             }
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
             rt.calculate_utilization(144, do_preload_until=3002))
        
        self.assertEqual(utilization_timestamps, [3002, 3003, 3005, 3010])
        self.assertEqual(utilization_values,     [96, 144,  48,   0])
        self.assertEqual(acc_waste,0)
        self.assertEqual(integrated_ut, corrected_ut)
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
             rt.calculate_utilization(144, do_preload_until=3003, 
                                      endCut=3006))
        
        self.assertEqual(utilization_timestamps, [3003, 3005, 3006])
        self.assertEqual(utilization_values,     [144,  48, 48])
        self.assertEqual(acc_waste,0)
        self.assertEqual(integrated_ut, corrected_ut)
        
        self.assertEqual(integrated_ut, (2.0*144.0+48.0)/(3.0*144.0))
        
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
        rt.calculate_utilization(144, do_preload_until=3003, 
                                      endCut=3010))
        
        self.assertEqual(utilization_timestamps, [3003, 3005, 3010])
        self.assertEqual(utilization_values,     [144,  48, 0])
        self.assertAlmostEqual(integrated_ut, 0.523809, delta=0.001)
        self.assertEqual(acc_waste,0)
        self.assertEqual(integrated_ut, corrected_ut)
        
        rt._wf_extractor=FakeWFExtractor()
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
                        rt.calculate_utilization(144, do_preload_until=3003, 
                                      endCut=3010))
        self.assertEqual(acc_waste,24)
        
        self.assertEqual(utilization_timestamps, [3003, 3005, 3006, 3007, 3008,
                                                  3010])
        self.assertEqual(utilization_values,     [144,  48,   36,   24,   48,    
                                                  0])
        self.assertAlmostEqual((integrated_ut-corrected_ut)*7*144, 24)
        #self.assertAlmostEqual(integrated_ut, 0.523809, delta=0.001)
    def test_utlization_sotre_load(self):
        rt = ResultTrace()
        self.addCleanup(self._del_table, "usage_values")
        rt._get_utilization_result().create_table(self._db)
        
        
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
             "time_submit": [3000,3000],
             "time_start": [3001,3003],
             "time_end": [3005,3010]                      
             }
        
        
        rt._wf_extractor=FakeWFExtractor()
        (integrated_ut, utilization_timestamps, utilization_values, acc_waste,
         corrected_ut) = (
                        rt.calculate_utilization(144, do_preload_until=3003, 
                                      endCut=3010, store=True, db_obj=self._db,
                                                trace_id=1))
        self.assertEqual(acc_waste,24)
        
        self.assertEqual(utilization_timestamps, [3003, 3005, 3006, 3007, 3008,
                                                  3010])
        self.assertEqual(utilization_values,     [144,  48,   36,   24,   48,    
                                                  0])
        rt_2 = ResultTrace()
        rt_2.load_utilization_results(self._db, 1)
        new_ut, new_acc, new_corrected_ut= rt_2.get_utilization_values()
        self.assertAlmostEqual(integrated_ut, new_ut)
        self.assertEqual(acc_waste, new_acc)
        print "new_corrected_ut", new_corrected_ut
        self.assertAlmostEqual(corrected_ut, new_corrected_ut)
    
    def test_calculate_utilization_median_result(self):
        rt = ResultTrace()
        self.addCleanup(self._del_table, "usage_values")
        rt._get_utilization_result().create_table(self._db)
        
        self._db.insertListValues("usage_values", ["trace_id", "type",
                                            "utilization","waste",
                                            "corrected_utilization"],
                                            [[1, "usage", 0.5, 10, 0.4 ],
                                            [2, "usage", 0.2, 11, 0.2 ],
                                            [3, "usage", 0.6, 9, 0.5 ],
                                            [4, "usage", 0.7, 13, 0.7 ]])
        
        rt.calculate_utilization_median_result([1,2,3,4], True, self._db,5)
        new_rt = ResultTrace()
        new_rt.load_utilization_results(self._db, 5)
        self.assertEqual(new_rt._acc_waste,10.5)
        self.assertEqual(new_rt._integrated_ut, 0.55)
        self.assertEqual(new_rt._corrected_integrated_ut, 0.45)
        
    def test_calculate_waiting_submitted_work(self):
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[2,1],
             "account": ["account2","account1", "account3"],
             "cpus_req": [1, 1, 1],
             "cpus_alloc": [1, 1, 1],
             "job_name":["jobName2", "jobName1", "jobName3"],
             "id_job": [2,1, 3],
             "id_qos": [3,2, 1],
             "id_resv": [4,3, 0],
             "id_user": [5,4, 1],
             "nodes_alloc": [4,2,3],
             "partition": ["partition2","partition1", "partition1"],
             "priority": [199, 99, 200],
             "state": [3,3,3],
             "timelimit": [200,100, 200],
             "time_submit": [2998,2999,3000],
             "time_start": [3001,3003, 3004],
             "time_end": [3005,3010, 3012]                      
             }
        rt._lists_start = {
             "job_db_inx":[2,1],
             "account": ["account2","account1", "account3"],
             "cpus_req": [1, 1, 1],
             "cpus_alloc": [1, 1, 1],
             "job_name":["jobName2", "jobName1", "jobName3"],
             "id_job": [2,1, 3],
             "id_qos": [3,2, 1],
             "id_resv": [4,3, 0],
             "id_user": [5,4, 1],
             "nodes_alloc": [4,2,3],
             "partition": ["partition2","partition1", "partition1"],
             "priority": [199, 99, 200],
             "state": [3,3,3],
             "timelimit": [200,100, 200],
             "time_submit": [2998,2999,3000],
             "time_start": [3001,3003, 3004],
             "time_end": [3005,3010, 3012]                      
             }
        
        stamps, waiting_ch, core_h_per_min_stamps, core_h_per_min_values = (
             rt.calculate_waiting_submitted_work(acc_period=0))
        self.assertEqual(stamps, [2998,2999,3000, 3001, 3003, 3004])
        self.assertEqual(waiting_ch, [4,11,19, 15, 8,0])
        self.assertEqual(core_h_per_min_stamps,
                         [2999,3000])
        self.assertEqual(core_h_per_min_values,
                         [11,9.5])
        
        

class FakeWFExtractor():
    def get_waste_changes(self):
        return [3006, 3007, 3008], [12, 12, -24], 24
        
class FakeDBObj:
    def __init__(self, test_obj):
        self._test_obj = test_obj
        self._id_count=0
        self._set_fields= []
        self._hist_count=0
        self._stats_count=0
    def insertValues(self, table, fields, values, get_insert_id=False):
        type_pos = fields.index("type")
        self._set_fields.append(values[type_pos])
        self._id_count+=1
        if table=="histograms":
            self._hist_count+=1
        if table=="numericStats":
            self._stats_count+=1
        return True, self._id_count

    def getValuesDicList(self, table_name, keys, condition):
        result_type = condition.split("'")[1]
        trace_id = int(condition.split("=")[1].split()[0])
        self._set_fields.append(result_type)
        self._test_obj.assertEqual(trace_id, 1)
        if table_name=="histograms":
            self._hist_count+=1
        if table_name=="numericStats":
            self._stats_count+=1
    
        