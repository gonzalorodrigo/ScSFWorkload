""" Unittests for the slurm simulator trace generator. 

 python -m unittest test_trace_gen
"""

import unittest
import slurm.trace_gen as trace_gen


class TestTraceGen(unittest.TestCase):
    
    def test_one_record(self):
        record=trace_gen.get_job_trace(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        f = open('tmp.trace', 'w')
        f.write(record)
        f.close()
        
        records=trace_gen.extract_records(file_name="tmp.trace",
                                list_trace_location="../bin/list_trace")
        # There should be only one record
        self.assertEqual(len(records), 1)
        read_record=records[0]
        
        self.assertEqual(read_record["JOBID"], "1");
        self.assertEqual(read_record["USERNAME"], "name");
        self.assertEqual(read_record["PARTITION"], "thepartition");
        self.assertEqual(read_record["ACCOUNT"], "theaccount");
        self.assertEqual(read_record["QOS"], "theqos");
        self.assertEqual(read_record["SUBMIT"], "1034");
        self.assertEqual(read_record["DURATION"], "102");
        self.assertEqual(read_record["WCLIMIT"], "101");
        self.assertEqual(read_record["TASKS"], "23(2,11)");
        self.assertEqual(read_record["NUM_TASKS"], 23);
        self.assertEqual(read_record["TASKS_PER_NODE"], 2);
        self.assertEqual(read_record["CORES_PER_TASK"], 11);
        self.assertEqual(read_record["PARTITION"], "thepartition");
        self.assertEqual(read_record["RES"], "thereservation");
        self.assertEqual(read_record["DEP"], "thedependency");
        
    def test_one_record_workflow(self):
        record=trace_gen.get_job_trace(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency",
                                       workflow_manifest="my_manifest.json")
        f = open('tmp.trace', 'w')
        f.write(record)
        f.close()
        
        records=trace_gen.extract_records(file_name="tmp.trace",
                                list_trace_location="../bin/list_trace")
        # There should be only one record
        print records
        self.assertEqual(len(records), 1)
        read_record=records[0]
        
        self.assertEqual(read_record["JOBID"], "1");
        self.assertEqual(read_record["USERNAME"], "name");
        self.assertEqual(read_record["PARTITION"], "thepartition");
        self.assertEqual(read_record["ACCOUNT"], "theaccount");
        self.assertEqual(read_record["QOS"], "theqos");
        self.assertEqual(read_record["SUBMIT"], "1034");
        self.assertEqual(read_record["DURATION"], "102");
        self.assertEqual(read_record["WCLIMIT"], "101");
        self.assertEqual(read_record["TASKS"], "23(2,11)");
        self.assertEqual(read_record["PARTITION"], "thepartition");
        self.assertEqual(read_record["RES"], "thereservation");
        self.assertEqual(read_record["DEP"], "thedependency");
        self.assertEqual(read_record["WF"], "my_manifest.json");
        
        
        
    
    def test_extract_records(self):
        records = trace_gen.extract_records(file_name="ref.trace",
                                list_trace_location="../bin/list_trace")
        self.assertGreater(len(records), 1)
        self.assertIsNot(records[0]["JOBID"], None)
        
    def test_dump_trace(self):
        generator = trace_gen.TraceGenerator()
        
        generator.add_job(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name2",
                                       submit_time=10342, 
                                       duration=1022,
                                       wclimit=1012,
                                       tasks = 232,
                                       cpus_per_task= 112,
                                       tasks_per_node= 22, 
                                       qosname="theqos2",
                                       partition="thepartition2",
                                       account="theaccount2",
                                       reservation="thereservation2",
                                       dependency="thedependency2")
        generator.dump_trace('tmp.trace')
        
        records=trace_gen.extract_records(file_name="tmp.trace",
                                list_trace_location="../bin/list_trace")
        # There should be only one record
        self.assertEqual(len(records), 2)
        read_record=records[0]
        
        self.assertEqual(read_record["JOBID"], "1");
        self.assertEqual(read_record["USERNAME"], "name");
        self.assertEqual(read_record["PARTITION"], "thepartition");
        self.assertEqual(read_record["ACCOUNT"], "theaccount");
        self.assertEqual(read_record["QOS"], "theqos");
        self.assertEqual(read_record["SUBMIT"], "1034");
        self.assertEqual(read_record["DURATION"], "102");
        self.assertEqual(read_record["WCLIMIT"], "101");
        self.assertEqual(read_record["TASKS"], "23(2,11)");
        self.assertEqual(read_record["PARTITION"], "thepartition");
        self.assertEqual(read_record["RES"], "thereservation");
        self.assertEqual(read_record["DEP"], "thedependency");
        
        read_record=records[1]
        self.assertEqual(read_record["JOBID"], "2");
        self.assertEqual(read_record["USERNAME"], "name2");
        self.assertEqual(read_record["PARTITION"], "thepartition2");
        self.assertEqual(read_record["ACCOUNT"], "theaccount2");
        self.assertEqual(read_record["QOS"], "theqos2");
        self.assertEqual(read_record["SUBMIT"], "10342");
        self.assertEqual(read_record["DURATION"], "1022");
        self.assertEqual(read_record["WCLIMIT"], "1012");
        self.assertEqual(read_record["TASKS"], "232(22,112)");
        self.assertEqual(read_record["PARTITION"], "thepartition2");
        self.assertEqual(read_record["RES"], "thereservation2");
        self.assertEqual(read_record["DEP"], "thedependency2");
        
    def test_dump_qos(self):
        generator = trace_gen.TraceGenerator()
        
        generator.add_job(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name2",
                                       submit_time=10342, 
                                       duration=1022,
                                       wclimit=1012,
                                       tasks = 232,
                                       cpus_per_task= 112,
                                       tasks_per_node= 22, 
                                       qosname="theqos2",
                                       partition="thepartition2",
                                       account="theaccount2",
                                       reservation="thereservation2",
                                       dependency="thedependency2")
        generator.dump_qos("qos.sim")
        f = file("qos.sim", "r")
        lines = f.readlines()
        self.assertEqual(len(lines), 2)
        
        self.assertEqual("theqos", lines[0].strip())
        self.assertEqual("theqos2", lines[1].strip())
        
    def test_dump_users(self):
        generator = trace_gen.TraceGenerator()
        
        generator.add_job(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name2",
                                       submit_time=10342, 
                                       duration=1022,
                                       wclimit=1012,
                                       tasks = 232,
                                       cpus_per_task= 112,
                                       tasks_per_node= 22, 
                                       qosname="theqos2",
                                       partition="thepartition2",
                                       account="theaccount2",
                                       reservation="thereservation2",
                                       dependency="thedependency2")
        generator.dump_users("users.sim")
        f = file("users.sim", "r")
        lines = f.readlines()
        self.assertEqual(len(lines), 2)
        
        self.assertEqual("name:1024", lines[0].strip())
        self.assertEqual("name2:1025", lines[1].strip())
        
    def test_get_core_s_per_period_s(self):
        generator = trace_gen.TraceGenerator()
        
        generator.add_job(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name2",
                                       submit_time=1037, 
                                       duration=1022,
                                       wclimit=1012,
                                       tasks = 232,
                                       cpus_per_task= 112,
                                       tasks_per_node= 22, 
                                       qosname="theqos2",
                                       partition="thepartition2",
                                       account="theaccount2",
                                       reservation="thereservation2",
                                       dependency="thedependency2")
        self.assertEqual(generator.get_submitted_core_s(),
                        (23*11*102+23*11*102+232*112*1022, 3.0))
        
    def test_get_core_s_per_period_s_decay(self):
        generator = trace_gen.TraceGenerator()
        generator.set_submitted_cores_decay(2)
        
        generator.add_job(job_id=1, username="name",
                                       submit_time=1034, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        generator.add_job(job_id=2, username="name",
                                       submit_time=1035, 
                                       duration=102,
                                       wclimit=101,
                                       tasks = 23,
                                       cpus_per_task= 11,
                                       tasks_per_node= 2, 
                                       qosname="theqos",
                                       partition="thepartition",
                                       account="theaccount",
                                       reservation="thereservation",
                                       dependency="thedependency")
        self.assertEqual(generator.get_submitted_core_s(),
                        (23*11*102+23*11*102, 1.0))
        generator.add_job(job_id=2, username="name2",
                                       submit_time=1038, 
                                       duration=1022,
                                       wclimit=1012,
                                       tasks = 232,
                                       cpus_per_task= 112,
                                       tasks_per_node= 22, 
                                       qosname="theqos2",
                                       partition="thepartition2",
                                       account="theaccount2",
                                       reservation="thereservation2",
                                       dependency="thedependency2")
        self.assertEqual(generator.get_submitted_core_s(),
                        (232*112*1022, 1.0))
        
      