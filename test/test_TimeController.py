"""
 python -m unittest test_TimeController
 

"""

from analysis import ProbabilityMap
from generate import TimeController
import datetime
import unittest

class TestTimeController(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        # This _gerenator will allways produce "3" as a number
        self._generator = ProbabilityMap(probabilities=[1.0],
                                         value_ranges=[(3,4)],
                                         interval_policy="low")
        
        self._controller =  TimeController(self._generator)
        
    def test_creation(self):
        self.assertEqual(self._controller._time_counter, 0)
        self.assertEqual(self._controller._start_date, 0)
        self.assertEqual(self._controller._run_limit, 0)
    def test_reset_time_counter(self):
        right_now =  int(datetime.datetime.now().strftime('%s'))
        self._controller.reset_time_counter()
        self.assertLess(self._controller._time_counter-right_now, 2)
        self.assertLess(self._controller._start_date-right_now, 2)
        
        new_date=int(datetime.datetime(2015,01,01).strftime('%s'))
        self._controller.reset_time_counter(datetime.datetime(2015,01,01))
        self.assertEqual(self._controller._time_counter,new_date)
        self.assertEqual(self._controller._start_date,new_date)
        
    def test_get_next_job_create_time(self):
        new_date=int(datetime.datetime(2015,01,01).strftime('%s'))
        self._controller.reset_time_counter(datetime.datetime(2015,01,01))
        
        self.assertEqual(new_date+3,
                        self._controller.get_next_job_create_time())
        self.assertEqual(new_date+6,
                        self._controller.get_next_job_create_time())
    
    def test_is_time_to_stop(self):
        new_date=int(TimeController.get_epoch(datetime.datetime(2015,01,01)))
        self._controller.reset_time_counter(datetime.datetime(2015,01,01))
        self._controller.set_run_limit(6)
        self.assertEqual(new_date+3,
                        self._controller.get_next_job_create_time())
        self.assertFalse(self._controller.is_time_to_stop())
        self.assertEqual(new_date+6,
                        self._controller.get_next_job_create_time())
        self._controller.get_next_job_create_time()
        self.assertTrue(self._controller.is_time_to_stop())
        
        
        
        