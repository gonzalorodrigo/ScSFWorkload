"""UNIT TESTS for the job variable analysis primitives

 python -m unittest test_ProbabilityMap
 

"""

import os
import random
import string
import unittest

from analysis import ProbabilityMap, _round_number


class TestProbabilityMap(unittest.TestCase):
    def test_init(self):
        prob_map = ProbabilityMap([0.2, 0.6, 0.9, 1.0],
                                  [(0,1),(2,3),(4,5), (7,8)],
                                  value_granularity=1.0, 
                                  interval_policy="midpoint",
                                  round_up=True)
        self.assertEqual(prob_map.get_probabilities(),
                         [0.2, 0.6, 0.9, 1.0])
        
        self.assertEqual(prob_map.get_value_ranges(),
                         [(0,1),(2,3),(4,5),(7,8)])
        self.assertEqual(prob_map.get_value_granularity(), 1.0)
        self.assertEqual(prob_map.get_interval_policy(), "midpoint")
        self.assertEqual(prob_map.get_round_up(), True)

    def test_save_load(self):
        prob_map = ProbabilityMap([0.2, 0.6, 0.9, 1.0],
                                  [(0,1),(2,3),(4,5),(10, 11)],
                                  value_granularity=1.0)
        file_name = "/tmp/"+gen_random_string()
        self.addCleanup(os.remove,file_name)
        prob_map.save(file_name)
        
        new_prob_map = ProbabilityMap([0.1, 0.6, 0.9, 1.0], 
                                      [(0,1),(2,3),(4,5), (7,8)])
        self.assertNotEqual(new_prob_map.get_probabilities(),
                            [0.2, 0.6, 0.9])
        
        self.assertNotEqual(new_prob_map.get_value_ranges(),
                            [(0,1),(2,3),(4,5),(10, 11)])
        self.assertNotEqual(new_prob_map.get_value_granularity(), 1.0)
        new_prob_map.load(file_name)
        self.assertEqual(new_prob_map.get_probabilities(),
                         [0.2, 0.6, 0.9,1.0])
        
        self.assertEqual(new_prob_map.get_value_ranges(),
                         [(0,1),(2,3),(4,5),(10, 11)])
        self.assertEqual(new_prob_map.get_value_granularity(), 1.0)

    def test_round_number(self):
        self.assertEqual(_round_number(11.3), 11.3)        
        self.assertEqual(_round_number(11.3, 2.0), 10)
        self.assertEqual(_round_number(-11.3, 2.0), -12)
        self.assertEqual(_round_number(-11.3, 2.0), -12)
        
        self.assertEqual(_round_number(11.3, 0.5), 11)
        self.assertEqual(_round_number(11.7, 0.5), 11.5)
        self.assertEqual(_round_number(11.7, 0.5, True), 12.0)
        
    def test_get_value_in_range(self):
        prob_map = ProbabilityMap([0.2, 0.6, 0.9,1.0],
                                  [(0,1),(2,3),(4,5), (7,9)],
                                  value_granularity=1.0)
        n = prob_map._get_value_in_range((1,2), policy="random")
        self.assertGreaterEqual(n, 1)
        self.assertLessEqual(n, 2)
        
        n = prob_map._get_value_in_range((1,2), policy="midpoint")
        self.assertEqual(n, 1.5)
        
        n = prob_map._get_value_in_range((1,2), policy="low")
        self.assertEqual(n, 1)
        
        n = prob_map._get_value_in_range((1,2), policy="high")
        self.assertEqual(n, 2)
    
    def test_get_range_for_number(self):
        prob_map = ProbabilityMap([0.2, 0.6, 0.9, 1.0], 
                                  [(0,1),(2,3),(4,5),(10,11)],
                                  value_granularity=1.0)
        self.assertEqual(prob_map._get_range_for_number(0), (0,1))    
        self.assertEqual(prob_map._get_range_for_number(0.1), (0,1))
        self.assertEqual(prob_map._get_range_for_number(0.2), (0,1))
        self.assertEqual(prob_map._get_range_for_number(0.4), (2,3))    
        self.assertEqual(prob_map._get_range_for_number(0.6), (2,3))
        self.assertEqual(prob_map._get_range_for_number(0.8), (4,5))
        self.assertEqual(prob_map._get_range_for_number(0.9), (4,5))
        self.assertEqual(prob_map._get_range_for_number(1.0), (10,11))
        self.assertRaises(ValueError, prob_map._get_range_for_number, 1.1)
        
        prob_map = ProbabilityMap([1.0], 
                                  [(0,1)],
                                  value_granularity=1.0)
        self.assertEqual(prob_map._get_range_for_number(0), (0,1))
        self.assertEqual(prob_map._get_range_for_number(0.5), (0,1))
        self.assertEqual(prob_map._get_range_for_number(1.0), (0,1))
    
    def test_get_number(self):
        prob_map_n = ProbabilityMap([0.2, 0.6, 0.9,1.0], 
                                  [(0,1),(2,3),(4,5),(10,11)],
                                  value_granularity=1.0)
        
        prob_map_n.random_gen.uniform=_fake_random
        
        self.assertEqual(prob_map_n.produce_number(), 4.7)
         
def _fake_random(num1, num2):
    return num1+(num2-num1)*0.7
        
def gen_random_string(N=10):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase +
                                            string.digits) for _ in range(N))
