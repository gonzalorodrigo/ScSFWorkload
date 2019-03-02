"""
 python -m unittest test_running
 

"""
from datetime import datetime
import os
import unittest

from commonLib.DBManager import DB
from commonLib.filemanager import ensureDir
from generate import TimeController
from machines import Edison2015
from orchestration.definition import ExperimentDefinition
from orchestration.running import ExperimentRunner
import slurm.trace_gen as trace_gen
from stats.trace import ResultTrace


class TestExperimentRunner(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
        ensureDir("./tmp")
        self._vm_ip = os.getenv("TEST_VM_HOST", "192.168.56.24")
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
        
    def test_conf(self):
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser",
                                   local_conf_dir="local_file",
                                   scheduler_conf_dir="sched_conf_dir",
                                   scheduler_conf_file_base="conf.file",
                                   scheduler_folder="folder",
                                   scheduler_script="script",
                                   manifest_folder="man_folder")
        
       
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/trace_folder")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
        self.assertEqual(ExperimentRunner._local, True)
        self.assertEqual(ExperimentRunner._run_hostname, "myhost")
        self.assertEqual(ExperimentRunner._run_user, "myUser")
        self.assertEqual(ExperimentRunner._local_conf_dir, "local_file")
        self.assertEqual(ExperimentRunner._scheduler_conf_dir, "sched_conf_dir")
        self.assertEqual(ExperimentRunner._scheduler_conf_file_base, 
                         "conf.file")
        self.assertEqual(ExperimentRunner._scheduler_folder, "folder")
        self.assertEqual(ExperimentRunner._scheduler_script, "script")
        self.assertEqual(ExperimentRunner._manifest_folder, "man_folder")
        
    
    def test_generate_trace_files(self):
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser",
                                   drain_time=0)
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/trace_folder")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
        self.assertEqual(ExperimentRunner._local, True)

                
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 400)
        
        er = ExperimentRunner(ed)
        er._generate_trace_files(ed)
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.qos"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.users"))
        records=trace_gen.extract_records(file_name=
                         "tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.trace",
                                list_trace_location="../bin/list_trace")
        man_count=0
        self.assertGreater(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 320)
        self.assertLess(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 1500)
        for rec in records:
            if rec["WF"].split("-")[0]=="manifestSim.json":
                man_count+=1
        self.assertGreaterEqual(man_count, 64, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
        self.assertLessEqual(man_count, 104, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
    
    def test_generate_trace_files_first_job(self):
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser",
                                   drain_time=0)
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/trace_folder")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
        self.assertEqual(ExperimentRunner._local, True)

                
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 400,
                 overload_target=3600000)
        
        er = ExperimentRunner(ed)
        er._generate_trace_files(ed)
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.qos"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.users"))
        records=trace_gen.extract_records(file_name=
                         "tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.trace",
                                list_trace_location="../bin/list_trace")
        man_count=0
        self.assertGreater(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 320)
        self.assertLess(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 1500+3720)
        for rec in records:
            if rec["WF"].split("-")[0]=="manifestSim.json":
                man_count+=1
        self.assertGreaterEqual(man_count, 64, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
        self.assertLessEqual(man_count, 104, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
        first_submit=TimeController.get_epoch(datetime(2016,1,1))-20-3600-120
        for i in range(360):
            self.assertEqual(int(records[i]["NUM_TASKS"]),
                             16*24)
            self.assertEqual(int(records[i]["DURATION"]), 7320)
            self.assertEqual(int(records[i]["WCLIMIT"]), 123)
            self.assertEqual(int(records[i]["SUBMIT"]), first_submit)
            first_submit+=10
        self.assertGreaterEqual(int(records[360]["SUBMIT"]), 
                                TimeController.get_epoch(datetime(2016,1,1))-20)
        self.assertNotEqual(int(records[360]["DURATION"]), 3600)
        
    
    def test_generate_trace_files_special(self):
        ExperimentRunner.configure(
                                       "tmp/trace_folder",
                                       "tmp", 
                                       True,
                                       "myhost", "myUser")
        ed = ExperimentDefinition(
                     seed="AAAA",
                     machine="edison",
                     trace_type="single",
                     manifest_list=[],
                     workflow_policy="sp-sat-p2-c24-r36000-t4-b100",
                     workflow_period_s=0,
                     workflow_handling="single",
                     preload_time_s = 0,
                     start_date = datetime(2016,1,1),
                     workload_duration_s = 120,
                     overload_target=1.2
                     )
        er = ExperimentRunner(ed)
        er._generate_trace_files(ed)
        trace_file_route=("tmp/{0}".format(ed.get_trace_file_name()))
        self.assertTrue(os.path.exists(trace_file_route))
        records=trace_gen.extract_records(file_name=trace_file_route,
                                    list_trace_location="../bin/list_trace")
        self.assertEqual(len(records), 8)
        submit_times=[0, 2,4,6, 100, 102, 104, 106]
        first_submit = int(records[0]["SUBMIT"])
        submit_times = [x+first_submit for x in submit_times]
        
        for (rec, submit_time) in zip(records, submit_times):
            self.assertEqual(int(rec["SUBMIT"]), submit_time)
            self.assertEqual(int(rec["NUM_TASKS"]) * 
                             int(rec["CORES_PER_TASK"]), 
                             24)
            self.assertEqual(int(rec["DURATION"]), 36000)
            self.assertEqual(int(rec["WCLIMIT"]), 601)
        
    
    def test_generate_trace_files_overload(self):
        
        for seed_string in ["seeeed", "asdsa", "asdasdasd", "asdasdasdas",
                            "asdasdlkjlkjl", "eworiuwioejrewk", "asjdlkasdlas"]:
            ExperimentRunner.configure(
                                       "tmp/trace_folder",
                                       "tmp", 
                                       True,
                                       "myhost", "myUser")
            self.assertEqual(ExperimentRunner._trace_folder,"tmp/trace_folder")
            self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
            self.assertEqual(ExperimentRunner._local, True)
    
            workload_duration=4*3600
            m=Edison2015()
            total_cores=m.get_total_cores()
            ed = ExperimentDefinition(
                     seed=seed_string,
                     machine="edison",
                     trace_type="single",
                     manifest_list=[],
                     workflow_policy="no",
                     workflow_period_s=0,
                     workflow_handling="single",
                     preload_time_s = 0,
                     start_date = datetime(2016,1,1),
                     workload_duration_s = workload_duration,
                     overload_target=1.2
                     )
            
            er = ExperimentRunner(ed)
            er._generate_trace_files(ed)
            trace_file_route=("tmp/{0}".format(ed.get_trace_file_name()))
            self.assertTrue(os.path.exists(trace_file_route))
            records=trace_gen.extract_records(file_name=trace_file_route,
                                    list_trace_location="../bin/list_trace")
            acc_core_hours=0
            for rec in records:
                acc_core_hours+=(int(rec["NUM_TASKS"]) 
                                  * int(rec["CORES_PER_TASK"])
                                  * int(rec["DURATION"]))
            
            print "pressure Index:", (float(acc_core_hours) / 
                                      float(total_cores*workload_duration))
            self.assertGreater(acc_core_hours, 
                                  1.1*total_cores*workload_duration)
            self.assertLess(acc_core_hours, 
                                   1.5*total_cores*workload_duration)
        
    def test_place_trace_files_local_and_clean(self):

        
        ExperimentRunner.configure(
                                   "tmp/dest",
                                   "tmp/orig", 
                                   True,
                                   "myhost", "myUser",
                                   scheduler_folder="./tmp/sched",
                                   scheduler_conf_dir="./tmp/conf",
                                   manifest_folder="manifests")
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/dest")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp/orig")
        self.assertEqual(ExperimentRunner._local, True)
        ensureDir("./tmp/orig")
        ensureDir("./tmp/dest")
        ensureDir("./tmp/sched")
        ensureDir("./tmp/conf")
        
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41,
                 overload_target=1.1)
        er = ExperimentRunner(ed)    
 
        filenames=er._generate_trace_files(ed)
        er._place_trace_file(filenames[0])
        er._place_users_file(filenames[2])
        self.assertTrue(os.path.exists(
                         "tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists(
                        "tmp/conf/users.sim"))
        
        self.assertFalse(os.path.exists(
                         "tmp/orig/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        
        self.assertFalse(os.path.exists(
                        "tmp/orig/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.users"))
        er.clean_trace_file()
        self.assertFalse(os.path.exists(
                         "tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        self.assertFalse(os.path.exists(
                        "tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.users"))
    def test_place_trace_files_remote_and_clean(self):
        ExperimentRunner.configure(
                                   "/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   True,
                                   "locahost", None,
                                   scheduler_folder="/tmp/tests/tmp/sched",
                                   scheduler_conf_dir="/tmp/tests/tmp/conf",
                                   manifest_folder="manifests")
        self.assertEqual(ExperimentRunner._trace_folder, 
                                        "/tmp/tests/tmp/dest")
        self.assertEqual(ExperimentRunner._trace_generation_folder,
                                        "/tmp/tests/tmp/orig")
        self.assertEqual(ExperimentRunner._local, True)
        ensureDir("/tmp/tests/tmp/dest")
        ensureDir("/tmp/tests/tmp/orig")
        ensureDir("/tmp/tests/tmp/sched")
        ensureDir("/tmp/tests/tmp/conf")
        
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41,
                 overload_target=1.1)
        er = ExperimentRunner(ed)
        filenames=er._generate_trace_files(ed)
        er._place_trace_file(filenames[0])
        er._place_users_file(filenames[2])
        self.assertTrue(os.path.exists(
                         "/tmp/tests/tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists(
                        "/tmp/tests/tmp/conf/users.sim"))
        self.assertFalse(os.path.exists(
                         "/tmp/tests/tmp/orig/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))

     
        er.clean_trace_file()
        self.assertFalse(os.path.exists(
                         "/tmp/tests/tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))

        self.assertFalse(os.path.exists(
                        "/tmp/tests/tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.users"))
    def test_configure_slurm(self):
        ExperimentRunner.configure("/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   True,
                                   "locahost", None,
                                   scheduler_conf_dir="tmp/conf",
                                   local_conf_dir="tmp/conf_orig")
        ensureDir("tmp/conf")
        ensureDir("tmp/conf_orig")
        if os.path.exists("tmp/conf/slurm.conf"):
            os.remove("tmp/conf/slurm.conf")
        orig=open("tmp/conf_orig/slurm.conf.edison.regular", "w")
        orig.write("regular")
        orig.close()
        
        orig=open("tmp/conf_orig/slurm.conf.edsion.wfaware", "w")
        orig.write("aware")
        orig.close()
        
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41)
        er = ExperimentRunner(ed)
        er._configure_slurm()
        final=open("tmp/conf/slurm.conf")
        line=final.readline()
        self.assertEqual("regular", line)
        final.close()
    
    def test_is_it_running(self):
        ExperimentRunner.configure(
                                   "/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   True,
                                   "locahost", None,
                                   scheduler_conf_dir="tmp/conf",
                                   local_conf_dir="tmp/conf_orig")
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41)
        er = ExperimentRunner(ed)
        self.assertTrue(er.is_it_running("python"))
        self.assertFalse(er.is_it_running("pythondd"))
    
    def test_is_it_running_failed_comms(self):
        ExperimentRunner.configure(
                                   "/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   False,
                                   "fakehost.fake.com", "aUSer",
                                   scheduler_conf_dir="tmp/conf",
                                   local_conf_dir="tmp/conf_orig")
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41)
        er = ExperimentRunner(ed)
        self.assertRaises(SystemError, er.is_it_running, "python")
        
    def test_run_simulation(self):
        ExperimentRunner.configure(trace_folder="/tmp/",
                                   trace_generation_folder="tmp", 
                                   local=False,
                                   run_hostname=self._vm_ip,
                                   run_user=None,
                                   scheduler_conf_dir="/scsf/slurm_conf",
                                   local_conf_dir="configs/",
                                   scheduler_folder="/scsf/",
                                   drain_time=100)
        ensureDir("tmp")
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 60,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 3600)
        er = ExperimentRunner(ed)
        er.create_trace_file()
        er._run_simulation()
        
        er.stop_simulation()
        self.assertTrue(er.is_simulation_done())
        
    def test_do_full_run(self):
        sched_db_obj = DB(self._vm_ip,
                          "slurm_acct_db",
                          os.getenv("SLURMDB_USER", None),
                          os.getenv("SLURMDB_PASS", None))
        trace = ResultTrace()
        self.addCleanup(self._del_table, "traces")
        trace.create_trace_table(self._db, "traces")
        
        ExperimentRunner.configure(trace_folder="/tmp/",
                                   trace_generation_folder="tmp", 
                                   local=False,
                                   run_hostname=self._vm_ip,
                                   run_user=None,
                                   scheduler_conf_dir="/scsf/slurm_conf",
                                   local_conf_dir="configs/",
                                   scheduler_folder="/scsf/",
                                   drain_time=100)
        ensureDir("tmp")
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 60,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 1800)
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        ed.store(self._db)
        
        er = ExperimentRunner(ed)
        self.assertTrue(er.do_full_run(sched_db_obj, self._db))
        