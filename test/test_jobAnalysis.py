"""UNIT TESTS for the job variable analysis primitives

 python -m unittest test_jobAnalysis
 
  python -m unittest test_jobAnalysis.TestJobAnalysis.test_produce_inter_times
 
"""

import os
import unittest

from analysis.jobAnalysis import (produce_inter_times, _join_var_bins,
                                  calculate_probability_map,
                                  calculate_histogram, get_jobs_data,
                                  get_jobs_data_trace)
from slurm import trace_gen as trace_gen


class TestJobAnalysis(unittest.TestCase):
    def test_produce_inter_times(self):
        created_values = [160, 220, 340, 340, 527]
        inter_values = produce_inter_times(created_values)
        self.assertEqual(inter_values, [60, 120, 0, 187], "Error calculating"
                         "inter arrival times from jobs' created times")
        
        self.assertRaises(ValueError, produce_inter_times,
                          [20, 30, 20])
        
    def test_calculate_histogram(self):
        values = [1, 2, 2, 3, 5]
        bins, edges = calculate_histogram(values, th_min=0,
                                          th_acc=1, 
                                          range_values=(0,6),
                                          interval_size=1)
        
        self.assertEqual(1, sum(bins),"Generated histogram components have "
                         "to sum 1")
        self.assertEqual(list(bins), 
                         [0,0.2,0.4,0.2,0,0.2,0.0])
        self.assertEqual(edges, 
                         [0,1,2,3,4,5,6,7])
        
    def test_join_var_bins(self):
        hist = [0.01, 0.01, 0.2, 0.3, 0.07, 0.09]
        bin_edges = [0, 1, 2, 3, 4, 5, 6]
        
        composed_hist, composed_edges = _join_var_bins(
                            hist,
                            bin_edges,
                            th_min=0.1,
                            th_acc=0.15)
        self.assertEqual(list(composed_hist),
                         [0.02, 0.2, 0.3, 0.07, 0.09], "small bins were not"
                         " joined correctly")
        self.assertEqual(list(composed_edges),
                         [0, 2, 3, 4, 5, 6], "small bins were not"
                         " joined correctly")
        
        # Testing with 0s
        hist = [0.01, 0, 0, 0.3, 0.07, 0]
        bin_edges = [0, 1, 2, 3, 4, 5, 6]
        
        composed_hist, composed_edges = _join_var_bins(
                            hist,
                            bin_edges,
                            th_min=0,
                            th_acc=1.0)
        self.assertEqual(list(composed_hist),
                         [0.01, 0, 0.3, 0.07, 0], "empty bins were not joined")
        self.assertEqual(list(composed_edges),
                         [0, 1, 3, 4, 5, 6], "empty bins were not joined")
        
        # Testing with 0s
        hist = [0.01, 0, 0, 0.3, 0, 0]
        bin_edges = [0, 1, 2, 3, 4, 5, 6]
        
        composed_hist, composed_edges = _join_var_bins(
                            hist,
                            bin_edges,
                            th_min=0,
                            th_acc=1.0)
        self.assertEqual(list(composed_hist),
                         [0.01, 0, 0.3, 0], "empty bins were not joined when "
                         "they are at the end of the list of bins")
        self.assertEqual(list(composed_edges),
                         [0, 1, 3, 4, 6])
        
    def test_calculate_probability_map(self):
        hist = [0.01, 0, 0, 0.3, 0, 0.69]
        bin_edges = [0, 1, 2, 3, 4, 5, 6]
        
        prob_map = calculate_probability_map(hist, bin_edges)
        self.assertEqual(prob_map.get_probabilities(),
                         [0.01,0.31,1.0])
        
        self.assertEqual(prob_map.get_value_ranges(),
                         [(0, 1),(3,4), (5,6)])
        
        hist = [1.0]
        bin_edges = [0, 1]
        
        prob_map = calculate_probability_map(hist, bin_edges)
        self.assertEqual(prob_map.get_probabilities(),
                         [1.0])
        
        self.assertEqual(prob_map.get_value_ranges(),
                         [(0,1)])
        
        prob_map = calculate_probability_map(hist, bin_edges,
                                             value_granularity=0.1)
        self.assertEqual(prob_map.get_value_granularity(), 0.1)
        
        
        
                
    def test_get_jobs_data_trace(self):
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
        
        data_dic = get_jobs_data_trace('tmp.trace', 
                                       "../bin/list_trace")
        
        self.assertEqual(data_dic["duration"], [102])
        self.assertEqual(data_dic["totalcores"], [253])
        self.assertEqual(data_dic["wallclock_requested"], [101])
        self.assertEqual(data_dic["created"], [1034])                          
                            
        
        