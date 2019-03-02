"""
 python -m unittest test_SpecialGeneratos
 

"""

import unittest
from generate.special import (FixedJobGenerator, SpecialGenerators,
                              SaturateGenerator,BFSaturateGenerator)

from generate.special.machine_filler import filler
class FakeWdGenSpecial():
    
    def __init__(self):
        self._times=[]
        self._cores=[]
        self._runtimes=[]
        self._wc_limits=[]
    def _generate_new_job(self, create_time, cores=None, run_time=None,
                          wc_limit=None, override_filter=False):
        self._times.append(create_time)
        self._cores.append(cores)
        self._runtimes.append(run_time)
        self._wc_limits.append(wc_limit)
    
    def disable_generate_workload_elemet(self):
        self._disabled=True
        
class TestFixedJobGenerator(unittest.TestCase):
    
    def test_init(self):
        wg = FakeWdGenSpecial()
        fjg = FixedJobGenerator(wg, 10, 11, 12)
        self.assertEqual(fjg._workload_generator, wg)
        self.assertEqual(fjg._cores, 10)
        self.assertEqual(fjg._run_time, 11)
        self.assertEqual(fjg._wc_limit, 12)
        
        fjg = FixedJobGenerator(wg, 10, 121, None)
        self.assertEqual(fjg._workload_generator, wg)
        self.assertEqual(fjg._cores, 10)
        self.assertEqual(fjg._run_time, 121)
        self.assertEqual(fjg._wc_limit, 4)
    
    def test_do_trigger(self):
        wg = FakeWdGenSpecial()
        fjg = FixedJobGenerator(wg, 10, 121, None)
        fjg.do_trigger(10)
        fjg.do_trigger(11)
        self.assertEqual(wg._times, [10, 11])
        self.assertEqual(wg._cores, [10, 10])
        self.assertEqual(wg._runtimes, [121, 121])
        self.assertEqual(wg._wc_limits, [4, 4])
        
class TestSpecialGenerators(unittest.TestCase):
    def setUp(self):
        self._wg = FakeWdGenSpecial()
    def test_get_generator(self):
        sg = SpecialGenerators.get_generator(
                                """sp-sat-p1-c24-r36000-t5576-b30424""",
                                self._wg, 10)
        self.assertEqual(self._wg, sg._workload_generator)
        self.assertEqual(sg._register_timestamp, 10)
        self.assertEqual(sg._current_timestamp, 10)
        self.assertEqual(sg._next_blast_time, 11)
        self.assertEqual(sg._next_job_time, 11)
        self.assertEqual(sg._jobs_submitted, 0)
        self.assertTrue(type(sg) is SaturateGenerator)
        
        sg = SpecialGenerators.get_generator(
                """sp-bf-p10-c24-r61160-t5756-b123520-g600-lc240-lr119920-wc133824-wr3600""",
                self._wg, 10)
        
        self.assertEqual(self._wg, sg._workload_generator)
        self.assertEqual(sg._register_timestamp, 10)
        self.assertEqual(sg._current_timestamp, 10)
        self.assertEqual(sg._next_blast_time, 11)
        self.assertTrue(type(sg) is BFSaturateGenerator)
        
    
    
class TestSaturateGenerator(unittest.TestCase):
    
    def setUp(self):
        self._wg = FakeWdGenSpecial()
    
    def test_init(self):
        sg = SaturateGenerator(self._wg, 10)
        self.assertEqual(self._wg, sg._workload_generator)
        self.assertEqual(sg._register_timestamp, 10)
        self.assertEqual(sg._current_timestamp, 10)
        self.assertEqual(sg._next_blast_time, 11)
        self.assertEqual(sg._next_job_time, 11)
        self.assertEqual(sg._jobs_submitted, 0)
        self.assertTrue(self._wg._disabled) 
    def test_register_time(self):
        sg = SaturateGenerator(self._wg, 10)
        sg.register_time(12)
        self.assertEqual(sg._next_blast_time, 13)
        self.assertEqual(sg._next_job_time, 13)
        self.assertEqual(sg._jobs_submitted, 0)
    def test_parse_desc(self):
        my_desc = """sp-sat-p1-c24-r36000-t5576-b30424"""
        sg = SaturateGenerator(self._wg, 10)
        sg.parse_desc(my_desc)
        self.assertEqual(sg._job_period, 1)
        self.assertEqual(sg._cores, 24)
        self.assertEqual(sg._run_time, 36000)
        self.assertEqual(sg._jobs_per_blast, 5576)
        self.assertEqual(sg._blast_period, 30424)
        
        self.assertEqual(sg._pattern_generator._cores, 24)
        self.assertEqual(sg._pattern_generator._run_time, 36000)
        self.assertEqual(sg._pattern_generator._wc_limit, 601)
        my_desc = """sp-sat-p100-c24-r36000-t5576-b30424"""
        self.assertRaises(ValueError, sg.parse_desc, my_desc)
    
    def test_do_trigger(self):
        my_desc = """sp-sat-p2-c24-r36000-t4-b100"""
        sg = SaturateGenerator(self._wg, 10)
        sg.parse_desc(my_desc)
        sg.do_trigger(12)
        self.assertEqual(self._wg._times, [11])
        self.assertEqual(self._wg._runtimes, [36000])
        self.assertEqual(self._wg._wc_limits, [601])
        self.assertEqual(self._wg._cores, [24])
        
        sg.do_trigger(17)
        self.assertEqual(self._wg._times, [11, 13,15,17])
        self.assertEqual(self._wg._runtimes, [36000,36000,36000,36000])
        self.assertEqual(self._wg._wc_limits, [601,601,601,601])
        self.assertEqual(self._wg._cores, [24,24,24,24])
        
        sg.do_trigger(20)
        self.assertEqual(self._wg._times, [11, 13,15,17])
        self.assertEqual(self._wg._runtimes, [36000,36000,36000,36000])
        self.assertEqual(self._wg._wc_limits, [601,601,601,601])
        self.assertEqual(self._wg._cores, [24,24,24,24])
        
        self.assertEqual(sg.do_trigger(111),1)
        self.assertEqual(self._wg._times, [11, 13,15,17, 111])
        self.assertEqual(self._wg._runtimes, [36000,36000,36000,36000, 36000])
        self.assertEqual(self._wg._wc_limits, [601,601,601,601,601])
        self.assertEqual(self._wg._cores, [24,24,24,24, 24])
        
        sg.do_trigger(120)
        self.assertEqual(self._wg._times, [11, 13,15,17, 111, 113, 115, 117])
        self.assertEqual(self._wg._runtimes, [36000,36000,36000,36000,
                                              36000,36000,36000,36000])
        self.assertEqual(self._wg._wc_limits, [601,601,601,601, 
                                               601,601,601,601])
        self.assertEqual(self._wg._cores, [24,24,24,24,
                                           24,24,24,24])
    
    
class TestBFSaturateGenerator(unittest.TestCase):
    
    def setUp(self):
        self._wg = FakeWdGenSpecial()
    
    def test_init(self):
        sg = BFSaturateGenerator(self._wg, 10)
        self.assertEqual(self._wg, sg._workload_generator)
        self.assertEqual(sg._register_timestamp, 10)
        self.assertEqual(sg._current_timestamp, 10)
        self.assertEqual(sg._next_blast_time, 11)
        self.assertEqual(sg._long_job_submit_time, 11)
        self.assertEqual(sg._wide_job_submit_time, 71)
        self.assertTrue(self._wg._disabled) 
    
    
    def test_register_time(self):
        sg = BFSaturateGenerator(self._wg, 10)
        sg.parse_desc("sp-bf-p10-c24-r59160-t5556-b119520-g600-lc240-lr115920"
                      "-wc133824-wr3600")
        sg.register_time(12)
        self.assertEqual(sg._next_blast_time, 13)
        self.assertEqual(sg._long_job_submit_time, 13)
        self.assertEqual(sg._wide_job_submit_time, 73)
        
        self.assertEqual(sg._small_jobs_generator._next_blast_time, 133)
        self.assertEqual(sg._small_jobs_generator._next_job_time, 133)
        self.assertEqual(sg._small_jobs_generator._jobs_submitted, 0)
        
  
    def test_parse_desc(self):
        my_desc = ("sp-bf-p10-c24-r59160-t5556-b119520-g600-lc240-lr115920"
                   "-wc133824-wr3600")
        sg = BFSaturateGenerator(self._wg, 10)
        sg.parse_desc(my_desc)
        self.assertEqual(sg._small_jobs_generator._job_period, 10)
        self.assertEqual(sg._small_jobs_generator._cores, 24)
        self.assertEqual(sg._small_jobs_generator._run_time, 59160)
        self.assertEqual(sg._small_jobs_generator._jobs_per_blast, 5556)
        self.assertEqual(sg._small_jobs_generator._blast_period, 119520)
        self.assertEqual(sg._small_jobs_generator._pattern_generator._cores, 24)
        self.assertEqual(sg._small_jobs_generator._pattern_generator._run_time,
                          59160)
        
        
        self.assertEqual(sg._blast_period, 119520)
        self.assertEqual(sg._gap, 600)
        self.assertEqual(sg._long_cores, 240)
        self.assertEqual(sg._long_runtime, 115920)
        self.assertEqual(sg._wide_cores, 133824)
        self.assertEqual(sg._wide_runtime, 3600)
        
        self.assertEqual(sg._long_job_generator._cores, 240)
        self.assertEqual(sg._long_job_generator._run_time, 115920)
        
        self.assertEqual(sg._wide_job_generator._cores, 133824)
        self.assertEqual(sg._wide_job_generator._run_time, 3600)
        
        
        
        
        
        my_desc = ("sp-bf-p10-c24-r59160-t5556-b222-g600-lc240-lr115920"
                   "-wc133824-wr3600")
        self.assertRaises(ValueError, sg.parse_desc, my_desc)
     
    def test_do_trigger(self):
        my_desc = ("sp-sat-p2-c24-r36000-t4-b10000-g30-lc240-lr1800"
                   "-wc1024-wr600")
        sg = BFSaturateGenerator(self._wg, 10)
        sg.parse_desc(my_desc)
        sg.register_time(10)
        self.assertEqual(sg.do_trigger(12),1)
        self.assertEqual(self._wg._times, [11])
        self.assertEqual(self._wg._runtimes, [1800])
        self.assertEqual(self._wg._wc_limits, [31])
        self.assertEqual(self._wg._cores, [240])
        
        
        self.assertEqual(sg.do_trigger(72), 1)
        self.assertEqual(self._wg._times, [11, 71])
        self.assertEqual(self._wg._runtimes, [1800, 600])
        self.assertEqual(self._wg._wc_limits, [31, 11])
        self.assertEqual(self._wg._cores, [240, 1024])
        
        self.assertEqual(sg.do_trigger(135),3)
        self.assertEqual(self._wg._times, [11, 71, 131, 133,135])
        self.assertEqual(self._wg._runtimes, [1800, 600,36000,36000,36000])
        self.assertEqual(self._wg._wc_limits, [31, 11,601,601,601])
        self.assertEqual(self._wg._cores, [240, 1024,24,24,24])
         
        
        sg.do_trigger(10000)
        self.assertEqual(self._wg._times, [11, 71, 131, 133,135,137])
        self.assertEqual(self._wg._runtimes, [1800, 600,36000,36000,36000,
                                              36000])
        self.assertEqual(self._wg._wc_limits, [31, 11,601,601,601,601])
        self.assertEqual(self._wg._cores, [240, 1024,24,24,24,24])
        
        sg.do_trigger(10135)
        self.assertEqual(self._wg._times, [11, 71, 131, 133,135,137,
                                           10011, 10071, 10131, 10133, 10135])
        self.assertEqual(self._wg._runtimes, [1800, 600,36000,36000,36000,
                                              36000,
                                              1800, 600,36000,36000,36000])
        self.assertEqual(self._wg._wc_limits, [31, 11, 601,601,601, 601,
                                               31, 11,601,601,601])
        self.assertEqual(self._wg._cores, [240, 1024,24,24,24,24,
                                           240, 1024,24,24,24])

class TestMachineFiller(unittest.TestCase):
    
    def test_filler(self):
        wg = FakeWdGenSpecial()
        filler(wg, start_time=110,target_wait=100, max_cores=500, 
               cores_per_node=5, job_separation=10, final_gap=5)
        self.assertEqual(wg._times, [5, 15, 25, 35, 45, 55, 65, 75, 85, 95])
        self.assertEqual(wg._cores, [50]*10)
        self.assertEqual(wg._runtimes, [205]*10)
        self.assertEqual(wg._wc_limits, [4]*10)
        
        
        