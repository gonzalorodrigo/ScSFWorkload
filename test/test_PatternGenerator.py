"""
 python -m unittest test_PatternGenerator
 

"""

from datetime import datetime as da
import datetime
import unittest

from generate import WorkloadGenerator, TimeController
from generate.overload import OverloadTimeController
from generate.pattern import PatternGenerator, WorkflowGenerator, \
    MultiAlarmTimer, WorkflowGeneratorMultijobs, RepeatingAlarmTimer, \
    WorkflowGeneratorSingleJob
from orchestration.definition import ExperimentDefinition
from orchestration.running import ExperimentRunner
from slurm.trace_gen import TraceGenerator
from test_WorkloadGenerator import (FakeTraceGenWF, FakeEdison,
                                    FakeTraceGenWFSimple)


class FakeWdGen():
    _times=[]
    def _generate_new_job(self, create_time):
        self._times.append(create_time)
        
class TestPatternGenerator(unittest.TestCase):
    
    def test_do_trigger(self):
        fwd = FakeWdGen()
        pg = PatternGenerator(fwd)
        self.assertEqual(pg.do_trigger(102),1)
        self.assertEqual(fwd._times, [102])
        self.assertEqual(pg.do_trigger(104),1)
        self.assertEqual(fwd._times, [102, 104])
    
    def test_init(self):
        wg = WorkloadGenerator(FakeEdison(),
                                     FakeTraceGenWF(self),
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        
        
        pg = PatternGenerator(wg)
        self.assertEqual(pg._workload_generator, wg)
        
        
    def test_init_workflow_gen(self):
        wg = WorkloadGenerator(FakeEdison(),
                                 FakeTraceGenWF(self),
                                 ["user1"],
                                 ["qos1"],
                                 ["partition1"],
                                 ["account1"])
        wg._job_id_counter=10
        wg._time_controller._time_counter=10001
        manifest_list=["manifest_sim.json"]
        share_list = [1.0]
        pg = WorkflowGenerator(manifest_list, share_list, wg)
        self.assertEqual(pg._manifest_selector._share_list, share_list)
        self.assertEqual(pg._manifest_selector._obj_list, manifest_list)
        self.assertEqual(pg._workload_generator, wg)
        self.assertEqual(1,pg.do_trigger(None))
        wg._job_id_counter=10
        self.assertEqual(1,pg.do_trigger(10001))
        
class TestMultiAlarmTimer(unittest.TestCase):
    
    def test_init(self):
        ma  = MultiAlarmTimer(None)
        self.assertEqual(ma._alarm_list, [])
        ma.register_time(100)
        self.assertEqual(ma._current_timestamp, 100)
        self.assertEqual(ma._register_timestamp, 100)
        
        ma  = MultiAlarmTimer(None, 200)
        self.assertEqual(ma._current_timestamp, 200)
        self.assertEqual(ma._register_timestamp, 200)
        base_time = TimeController.get_epoch(da(1970,1,1,00,00,00))
        ma = MultiAlarmTimer(None,
                             register_datetime=datetime.datetime(1970,1,1,00,00,
                                                                 10))
        self.assertEqual(ma._current_timestamp, base_time+10)
        self.assertEqual(ma._register_timestamp, base_time+10)
                             
    
    def test_set_alarm_list(self):
        ma = MultiAlarmTimer(None, 80)
        ma.set_alarm_list([101, 102, 103])
        self.assertFalse(ma.is_it_time(90))
        self.assertTrue(ma.is_it_time(101))
        self.assertTrue(ma.is_it_time(102))
        self.assertTrue(ma.is_it_time(103))
        self.assertFalse(ma.is_it_time(104))
        self.assertEqual(ma._current_timestamp, 104)
        self.assertRaises(ValueError, ma.set_alarm_list, [100, 101])
        self.assertRaises(ValueError, ma.set_alarm_list, [101, 100])
        
    def test_set_delta_list(self):
        ma = MultiAlarmTimer(None, 80)
        ma.set_delta_alarm_list([21,22,30])
        self.assertFalse(ma.is_it_time(90))
        self.assertTrue(ma.is_it_time(101))
        self.assertTrue(ma.is_it_time(102))
        self.assertFalse(ma.is_it_time(109))
        self.assertTrue(ma.is_it_time(110))
        self.assertFalse(ma.is_it_time(120))
        
        self.assertRaises(ValueError, ma.set_delta_alarm_list, [11, 10])
    
    def test_set_alarm_list_date(self):
        
        base_time = TimeController.get_epoch(da(1970,1,1,00,00,00))
        ma = MultiAlarmTimer(None, 
                             register_datetime=datetime.datetime(1970,1,1,00,00,
                                                                 10))
        ma.set_alarm_list_date([da(1970,1,1,00,00,12),da(1970,1,1,00,00,20)])
        self.assertFalse(ma.is_it_time(base_time+11))
        self.assertTrue(ma.is_it_time(base_time+12))
        self.assertFalse(ma.is_it_time(base_time+15))
        self.assertTrue(ma.is_it_time(base_time+20))
        
    def test_do_trigger(self):
        gen =FakeGen(None)
        base_time = TimeController.get_epoch(da(1970,1,1,00,00,00))
        ma = MultiAlarmTimer(gen, 
                     register_datetime=datetime.datetime(1970,1,1,00,00,
                                                         10))
        ma.set_alarm_list_date([da(1970,1,1,00,00,12),da(1970,1,1,00,00,12),
                                da(1970,1,1,00,00,20)])
        self.assertEqual(ma.do_trigger(base_time+11), 0)
        self.assertEqual(ma.do_trigger(base_time+12), 2)
        self.assertEqual(ma.do_trigger(base_time+15), 0)
        self.assertEqual(ma.do_trigger(base_time+20), 1)
        self.assertEqual(gen._count, 3)
        
class TestRepeatingAlarmTimer(unittest.TestCase):
    def test_set_alarm_period(self):
        rat = RepeatingAlarmTimer(None, 1000)
        rat.set_alarm_period(33)
        self.assertEqual(rat._alarm_period, 33)
    
    def test_is_it_time(self):
        rat = RepeatingAlarmTimer(None, 1000)
        rat.set_alarm_period(20)
        
        self.assertEqual(rat.is_it_time(1019),0)
        self.assertFalse(rat.can_be_purged())
        self.assertGreater(rat.is_it_time(1020),0)
        self.assertFalse(rat.can_be_purged())
        self.assertEqual(rat.is_it_time(1021),0)
        self.assertFalse(rat.can_be_purged())
        self.assertEqual(rat.is_it_time(1039),0)
        self.assertFalse(rat.can_be_purged())
        self.assertGreater(rat.is_it_time(1040),0)
        self.assertFalse(rat.can_be_purged())
        
class TestWorkflowGeneratorMultijobs(unittest.TestCase):
    def test_parse_all_jobs(self):
        wg = WorkloadGenerator(FakeEdison(),
                                 FakeTraceGenWF(self),
                                 ["user1"],
                                 ["qos1"],
                                 ["partition1"],
                                 ["account1"])
        wf = WorkflowGeneratorMultijobs(["manifest_sim.json"], [1.0],wg)
        cores, runtime, tasks= WorkflowGeneratorMultijobs.parse_all_jobs(
                                                        "manifest_sim.json")
        self.assertEqual(cores, 144)
        self.assertEqual(runtime, 960)
        self.assertEqual(len(tasks),2)
        task1= tasks["Decode"]
        task2= tasks["Hello"]
        self.assertEqual(task1["id"],"Decode")
        self.assertEqual(task1["number_of_cores"],112)
        self.assertEqual(task1["name"],"Decode")
        self.assertEqual(task1["runtime_limit"], 480)
        self.assertEqual(task1["runtime_sim"], 120)
        self.assertEqual(task1["execution_cmd"], "python ./Decode.py") 
        self.assertEqual(task1["dependencyFrom"], [])
        self.assertEqual(task1["dependencyTo"], [task2])
        
        self.assertEqual(task2["id"],"Hello")
        self.assertEqual(task2["number_of_cores"],144)
        self.assertEqual(task2["name"],"Hello")
        self.assertEqual(task2["runtime_limit"], 480)
        self.assertEqual(task2["runtime_sim"], 100)
        self.assertEqual(task2["execution_cmd"], "python ./Hello.py") 
        self.assertEqual(task2["dependencyFrom"], [task1])
        self.assertEqual(task2["dependencyTo"], [])
        
    def test_parse_expand_workflow(self):
        tg = FakeTraceGenWFSimple(self)
        wg = WorkloadGenerator(FakeEdison(),
                                 tg,
                                 ["user1"],
                                 ["qos1"],
                                 ["partition1"],
                                 ["account1"])
        wf = WorkflowGeneratorMultijobs(["manifest_sim.json"], [1.0],wg)
        self.assertEqual(wf.do_trigger(10000), 2)
        self.assertEqual(tg._job_count, 2)    
        self.assertEqual(tg._dep_count, 1)
        self.assertEqual(tg._manifests,
                         ["|wf_manifest_sim.json-1_Decode",
                          "|wf_manifest_sim.json-1_Hello_dDecode"])
                         
    def test_geb_deps(self):
        tg = FakeTraceGenWFSimple(self)
        wg = WorkloadGenerator(FakeEdison(),
                                 tg,
                                 ["user1"],
                                 ["qos1"],
                                 ["partition1"],
                                 ["account1"])
        wf = WorkflowGeneratorMultijobs(["manifest_sim.json"], [1.0],wg)
    
        task = {"dependencyFrom": [{"job_id":1},{"job_id":2}]}
        self.assertEqual(wf._gen_deps(task), "afterok:1,afterok:2")
        task = {"dependencyFrom": []}
        self.assertEqual(wf._gen_deps(task), "")
    def test_task_can_run(self):
        tg = FakeTraceGenWFSimple(self)
        wg = WorkloadGenerator(FakeEdison(),
                                 tg,
                                 ["user1"],
                                 ["qos1"],
                                 ["partition1"],
                                 ["account1"])
        wf = WorkflowGeneratorMultijobs(["manifest_sim.json"], [1.0],wg)
        
        task = {"dependencyFrom": [{"job_id":1},{"job_id":2}]}
        self.assertTrue(wf._task_can_run(task))
        task = {"dependencyFrom": []}
        self.assertTrue(wf._task_can_run(task), "")
        task = {"dependencyFrom": [{"job_id":1},{}]}
        self.assertFalse(wf._task_can_run(task))

        
class TestWorkflowGeneratorSingle(unittest.TestCase):
    def test_parse_expand_workflow(self):
        tg = FakeTraceGenWFSimple(self)
        wg = WorkloadGenerator(FakeEdison(),
                                 tg,
                                 ["user1"],
                                 ["qos1"],
                                 ["partition1"],
                                 ["account1"])
        wf = WorkflowGeneratorSingleJob(["manifest_sim.json"], [1.0],wg)
        self.assertEqual(wf.do_trigger(10000), 1)
        self.assertEqual(wf.do_trigger(20000), 1)
        self.assertEqual(tg._job_count, 2)    
        self.assertEqual(tg._dep_count, 0)
        self.assertEqual(tg._manifests,
                         ["manifest_sim.json-1",
                          "manifest_sim.json-2"])

class MyTraceGen(TraceGenerator):
    
    def add_job(self, job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest=None,
                  cores_s=None, real_core_s=None, ignore_work=False):
        count=super(MyTraceGen,
                    self).add_job(job_id=job_id, username=username,
                    submit_time=submit_time, duration=duration, wclimit=wclimit,
                    tasks=tasks,cpus_per_task=cpus_per_task,
                    tasks_per_node=tasks_per_node, qosname=qosname,
                    partition=partition, account=account,
                  reservation=reservation, dependency=dependency,
                   workflow_manifest= workflow_manifest)
        if not hasattr(self, "core_h_acc"):
            self.core_h_acc=[]
            self.core_h_stamps=[]
        if cores_s:
            self.core_h_acc.append(cores_s)
        else:
            self.core_h_acc.append(cpus_per_task*tasks*min(duration,
                                                           wclimit*60))
        self.core_h_stamps.append(submit_time)
        return count
    
    def check_pressure(self, max_cores, time_window, target_pressure, test_obj,
                       upper_margin):
        
        
        cores=self.core_h_acc
        stamps=self.core_h_stamps
                
        first_index=0
        last_index=0
        first_submit_time=stamps[first_index]
        last_submit_time=stamps[last_index]
        acc=cores[first_index]
        while last_index<len(stamps)-1:
            once=True
            while (once or (last_index<len(stamps)-1 and 
                   last_submit_time==stamps[last_index+1])):
                once=False
                last_index+=1
                last_submit_time=stamps[last_index]
                acc+=cores[last_index]
            
            while (first_index<last_index and
                   last_submit_time-first_submit_time>time_window):
                acc-=cores[first_index]
                first_index+=1
                first_submit_time=stamps[first_index]
            if (last_submit_time-first_submit_time>time_window/2):
                pressure=float(acc)/float(max_cores *
                                          (last_submit_time-first_submit_time))
                test_obj.assertGreater(pressure, target_pressure)
                test_obj.assertLess(pressure, target_pressure+upper_margin)
        
        return sum(cores), stamps[-1]-stamps[0]
                 
class TestOverloadTimeController(unittest.TestCase):
    def test_is_it_time(self):
        oc = OverloadTimeController(None, 0)
        fake_trace_gen = FrakeTraceGen()
        
        oc.configure_overload(fake_trace_gen,
                              100,
                              1.5)
        self.assertEqual(oc.is_it_time(10), 1)
        self.assertEqual(oc.is_it_time(12), 1)
        self.assertEqual(oc.is_it_time(24), 1)
        
    def test_sustained_levels(self):
        
        definition = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[],
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_handling="single",
                 preload_time_s = 3600*4,
                 workload_duration_s=3600*1,
                 overload_target=1.5)
        
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser")
        
        trace_generator = MyTraceGen()
        machine=definition.get_machine()    
        er = ExperimentRunner(definition)
        er._generate_trace_files(definition,trace_generator=trace_generator)
    
        acc_cores, period=trace_generator.check_pressure(machine.get_total_cores(),
                                       3600, 1.5, self, 1.0)
        total_pressure=float(acc_cores)/float(period*machine.get_total_cores())
        print total_pressure
        self.assertAlmostEqual(total_pressure, 1.5, delta=0.01)
        self.assertLess(total_pressure, 1.8)

        
    
    
    
    def test_is_it_time_hysteresis(self):
        oc = OverloadTimeController(None, 0)
        fake_trace_gen = FrakeTraceGen()
        
        oc.configure_overload(fake_trace_gen,
                              100,
                              1.0)
        fake_trace_gen._core_s=1000
        self.assertEqual(oc.is_it_time(10), 0)
        fake_trace_gen._core_s=960
        self.assertEqual(oc.is_it_time(20), 1)
        fake_trace_gen._core_s=940
        self.assertEqual(oc.is_it_time(30), 1)
        fake_trace_gen._core_s=1000
        self.assertEqual(oc.is_it_time(40), 0)
        fake_trace_gen._core_s=1060
        self.assertEqual(oc.is_it_time(40), 0)
        fake_trace_gen._core_s=1000
        self.assertEqual(oc.is_it_time(50), 0)
        fake_trace_gen._core_s=960
        self.assertEqual(oc.is_it_time(60), 1)
        fake_trace_gen._core_s=940
        self.assertEqual(oc.is_it_time(70), 1)
        
   
    
    def test_can_be_purged(self):
        oc = OverloadTimeController(None, 0)
        fake_trace_gen = FrakeTraceGen()
        
        oc.configure_overload(fake_trace_gen,
                              10000,
                              1.5)
        self.assertFalse(oc.can_be_purged())
        fake_trace_gen._core_s=15000
        fake_trace_gen._runtime=1
        self.assertFalse(oc.can_be_purged())
        
        fake_trace_gen._core_s=15000
        fake_trace_gen._runtime=2
        self.assertFalse(oc.can_be_purged())
        
        fake_trace_gen._core_s=45000
        fake_trace_gen._runtime=3
        self.assertFalse(oc.can_be_purged())
       
        
class FrakeTraceGen():
    _core_s=1000
    _runtime=10
    def get_submitted_core_s(self):
        return self._core_s, self._runtime
    def set_submitted_cores_decay(self, value):
        pass
    
class FakeGen(PatternGenerator):
    _count=0
    def do_trigger(self, create_time):
        self._count+=1
        return 1
        