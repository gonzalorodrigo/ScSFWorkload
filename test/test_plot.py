
"""UNIT TESTS for the the plotting functions

 python -m unittest test_plot
 
 
"""

from stats import Histogram, NumericStats

import numpy as np
import os
import unittest
from plot import histogram_cdf

class TestPlot(unittest.TestCase):
    
    def test_histogram_cdf(self):
        histogram_cdf([-1, 0.5, 1, 2], {"h1":[0.25, 0.15, 0.6],
                                        "h2":[0.25, 0.25, 0.5]}, "Test Hist",
                      "tmp/testhist", "x_label", "y_label", do_cdf=True,
                      y_log_scale=False,
                      cdf_y_log_scale=False)
        self.assertTrue(os.path.exists("tmp/testhist.png"))
        