"""
 python -m unittest test_WorkloadGenerator
 

"""

from datetime import datetime as da
from generate import WorkloadGenerator
from generate.pattern import WorkflowGenerator, MultiAlarmTimer
from machines import Edison
from slurm.trace_gen import TraceGenerator
import analysis
import random_control

import datetime
import unittest
from time import strftime

class TestWorkloadGenerator(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self._tg=FakeTraceGen(self)
        self._machine=FakeEdison()
        self._wg = WorkloadGenerator(self._machine,
                                     self._tg,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        
    def test_reproduce(self):
        
        common_seed="MYSEED"
        print "CC", common_seed.__hash__()
        tg1 = TraceGenerator()
        random_control.set_global_random_gen("AAAAA")
        
        
        wg1 = WorkloadGenerator(Edison(),
                                     tg1,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        wg1.generate_trace(datetime.datetime(2015,1,1),
                           1000, 1000)
        
        random_control.set_global_random_gen("AAAAA")
        
        tg2 = TraceGenerator()
        wg2 = WorkloadGenerator(Edison(),
                                     tg2,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        wg2.generate_trace(datetime.datetime(2015,1,1),
                           1000, 1000)
        self.assertEqual(tg1._job_list, tg2._job_list)
        
        random_control.set_global_random_gen("BBB")
        
        tg3 = TraceGenerator()
        wg3 = WorkloadGenerator(Edison(),
                                     tg3,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        wg3.generate_trace(datetime.datetime(2015,1,1),
                           1000, 1000)
        self.assertNotEqual(tg3._job_list, tg2._job_list)
        
        
    def test_init(self):
        self.assertEqual(self._tg, self._wg._trace_generator)
        self.assertEqual(self._machine, self._wg._machine)
        self.assertEqual(self._wg._job_id_counter,1)
        self.assertEqual(self._wg._user_list,["user1"])
        self.assertEqual(self._wg._qos_list,["qos1"])
        self.assertEqual(self._wg._partition_list,["partition1"])
        self.assertEqual(self._wg._account_list,["account1"])
        
    
    def test_generate_new_job(self):
        self._wg._job_id_counter = 10
        self._wg._generate_new_job(10001)
    
    def test_generate_trace(self):
        new_date=int(datetime.datetime(2015,01,01).strftime('%s'))
        run_limit=10
        self._tg._long_test=True
        self._wg.generate_trace(datetime.datetime(2015,01,01), run_limit)
        self.assertLessEqual(self._tg._add_job_calls, 10)
        self.assertGreaterEqual(self._tg._add_job_calls, 7)
    
    def test_generate_trace_job_count(self):
        new_date=int(datetime.datetime(2015,01,01).strftime('%s'))
        run_limit=100000
        self._tg._long_test=True
        self._wg.generate_trace(datetime.datetime(2015,01,01), run_limit,
                                job_limit=5)
        self.assertEqual(self._tg._add_job_calls, 5)
        
    def test_share_wf_gen(self):
        tg=FakeTraceGenWF(self)
        machine=FakeEdison()
        wg = WorkloadGenerator(machine,
                                     tg,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        flow = WorkflowGenerator(["manifest_sim.json"], [1.0], wg)
        wg.register_pattern_generator_share(flow, 2100.3)
        self.assertEqual(wg._workload_selector._max_pressure, 2.1)
        self.assertAlmostEqual(wg._workload_selector._prob_list[1]-
                         wg._workload_selector._prob_list[0], 0.3)
#         wg.generate_trace(datetime.datetime(2015,1,1),
#                           100,
#                           100)
#         
#         self.assertGreater(tg._workflow_count,10)
#         self.assertGreater(tg._job_count,10)

    def test_share_wf_gen_no_cap(self):
        tg=FakeTraceGenWF(self)
        machine=FakeEdison()
        wg = WorkloadGenerator(machine,
                                     tg,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        flow = WorkflowGenerator(["manifest_sim.json"], [1.0], wg)
        wg.register_pattern_generator_share(flow, 0.3)
        self.assertEqual(wg._workload_selector._max_pressure, 1.1)
        self.assertAlmostEqual(wg._workload_selector._prob_list[1]-
                         wg._workload_selector._prob_list[0], 0.3)
#         wg.generate_trace(datetime.datetime(2015,1,1),
#                           100,
#                           100)
#         
#         self.assertGreater(tg._workflow_count,10)
#         self.assertGreater(tg._job_count,10)
        
    def test_timed_wf_gen(self):
        
        tg=FakeTraceGenWF(self)
        machine=FakeEdison()
        wg = WorkloadGenerator(machine,
                                     tg,
                                     ["user1"],
                                     ["qos1"],
                                     ["partition1"],
                                     ["account1"])
        flow = WorkflowGenerator(["manifest_sim.json"], [1.0], wg)
       
      
        
        
        alarm = MultiAlarmTimer(flow,
                                register_datetime=datetime.datetime(2015,1,1))
        alarm.set_alarm_list_date([da(2015,1,1,00,00,10),da(2015,1,1,00,00,10),
                                  da(2015,1,1,00,00,20)])
        alarm2  = MultiAlarmTimer(flow,
                                register_datetime=datetime.datetime(2015,1,1))
        alarm2.set_alarm_list_date([da(2015,1,1,00,00,10),da(2015,1,2,00,00,10),
                                  da(2015,1,2,00,00,20)])
        wg.register_pattern_generator_timer(alarm)
        wg.register_pattern_generator_timer(alarm2)
        wg.generate_trace(datetime.datetime(2015,1,1),
                          100,
                          100)
        
        self.assertEqual(tg._workflow_count,4, "Alarms didn't generate WFs")
        self.assertGreater(tg._job_count,10)
        
        self.assertEqual(len(wg._pattern_timers),1, "Purging does not work")
    
    def test_save_trace(self):
        self._wg.save_trace("route")
        self.assertEqual(self._tg._dump_trace_calls,1)
        self.assertEqual(self._tg._dump_users_calls,1)
        self.assertEqual(self._tg._dump_qos_calls,1)

        
        
        

class FakeTraceGen(object):
    def __init__(self, test_obj, runtime=30, wclimit=2, cores=24):
        self._to=test_obj
        self._add_job_calls=0
        self._long_test=False
        self._dump_trace_calls=0
        self._dump_users_calls=0
        self._dump_qos_calls=0
        self._expected_wclimit = wclimit
        self._expected_runtime = runtime
        self._expected_cores = cores
    def add_job(self, job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest="",
                  cores_s=None, real_core_s=None, ignore_work=False):
        self._add_job_calls+=1
        if self._long_test:
            return
        self._to.assertEqual(job_id, 11)
        self._to.assertEqual(username, "user1")
        self._to.assertEqual(submit_time, 10001)
        self._to.assertLess(abs(duration-60), self._expected_runtime)
        self._to.assertLess(abs(wclimit-2), self._expected_wclimit)
        self._to.assertEqual(tasks, self._expected_cores)
        self._to.assertEqual(cpus_per_task, 1)
        self._to.assertEqual(tasks_per_node, 24)
        self._to.assertEqual(qosname, "qos1")
        self._to.assertEqual(partition, "partition1")
        self._to.assertEqual(account, "account1")
        self._to.assertEqual(reservation, "")
        self._to.assertEqual(dependency, "")
        self._to.assertEqual(workflow_manifest, "|")
    
    def dump_trace(self, filename):
        self._dump_trace_calls+=1
        self._to.assertEqual(filename, "route.trace")
    def dump_users(self, filename):
        self._dump_users_calls+=1
        self._to.assertEqual(filename, "route.users")
    def dump_qos(self, filename):
        self._dump_qos_calls+=1
        self._to.assertEqual(filename, "route.qos")




class FakeTraceGenWF(object):
    def __init__(self, test_obj):
        self._to=test_obj
        self._add_job_calls=0
        self._long_test=False
        self._dump_trace_calls=0
        self._dump_users_calls=0
        self._dump_qos_calls=0
        self._job_count=0;
        self._workflow_count=0
        self._dep_jobs=0
    def add_job(self, job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest="",
                  cores_s=None, real_core_s=None, ignore_work=False):
                  
        self._add_job_calls+=1
        if self._long_test:
            return
        
        
        if workflow_manifest=="|":
            self._job_count+=1
        else:
            self._workflow_count+=1
            self._to.assertEqual(workflow_manifest,
                    "manifest_sim.json-{0}".format(self._workflow_count))
            self._to.assertLess(abs(duration-60), 960)
            self._to.assertLess(abs(wclimit-2), 960/60)
            self._to.assertEqual(tasks, 144)
        self._to.assertEqual(username, "user1")
        self._to.assertEqual(cpus_per_task, 1)
        self._to.assertEqual(tasks_per_node, 24)
        self._to.assertEqual(qosname, "qos1")
        self._to.assertEqual(partition, "partition1")
        self._to.assertEqual(account, "account1")
        self._to.assertEqual(reservation, "")
        self._to.assertEqual(dependency, "")
        
    def dump_trace(self, filename):
        self._dump_trace_calls+=1
        self._to.assertEqual(filename, "route.trace")
    def dump_users(self, filename):
        self._dump_users_calls+=1
        self._to.assertEqual(filename, "route.users")
    def dump_qos(self, filename):
        self._dump_qos_calls+=1
        self._to.assertEqual(filename, "route.qos")
    
    def get_total_actual_cores_s(self):
        return 1

    def get_share_wfs(self):
        return 0
        
class FakeTraceGenWFSimple(FakeTraceGenWF):
    def __init__(self, test_obj):
        self._dep_count=0
        self._job_id=1
        self._job_count=0
        self._dump_trace_calls=0
        self._dump_users_calls=0
        self._dump_qos_calls=0
        self._job_ids=[]
        self._manifests=[]
    
    def add_job(self, job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest="",
                  cores_s=None, real_core_s=None, ignore_work=False):
        self._job_id+=1
        if dependency!="":
            self._dep_count+=1
        self._job_count+=1
        self._job_ids.append(self._job_id)
        self._manifests.append(workflow_manifest)
        return self._job_id
        
        
        

class FakeEdison(Edison):
    def __init__(self):
        super(FakeEdison,self).__init__()
        create_times = [2, 3, 4, 5, 6]
        self._generators["inter"] = (
                        self._populate_inter_generator(create_times))
        
        cores = [24,24,24,24] 
        self._generators["cores"] = (
                        self._populate_cores_generator(cores))
        
        wallclock = [120,120,120,120] 
        self._generators["wc_limit"] = \
            self._populate_wallclock_limit_generator(wallclock)

        wallclock = [120,120,120,120]
        runtime = [60, 60, 60, 60] 
        self._generators["accuracy"] = \
             self._populate_wallclock_accuracy(wallclock,
                                                              runtime)
        