"""
 python -m unittest test_RandomSelector
 

"""


import unittest
from generate import RandomSelector

class TestPatternGenerator(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self._rd = FakeRandom(0.3)
        self._sel =  RandomSelector(self._rd)
    def test_init(self):
        rd=FakeRandom(0.3)
        sel = RandomSelector(rd)
        self.assertEqual(sel._random_gen, rd)
        self.assertEqual(sel._share_list, [])
        self.assertEqual(sel._obj_list, [])
        self.assertEqual(sel._prob_list, [])
        self.assertEqual(sel._remaining_obj, None)
        self.assertEqual(sel.get_random_obj(), None)
    
    def test_set_remove_remaining(self):
        sel = RandomSelector(self._rd)
        sel.set_remaining("OBJ1")
        for a in range(100):
            self.assertEqual(sel.get_random_obj(), "OBJ1")
        self.assertEqual(sel._prob_list, [1.0])
        self.assertEqual(sel._obj_list, ["OBJ1"])
        
        sel.remove_remaining()
        self.assertEqual(sel._prob_list, [])
        self.assertEqual(sel._obj_list, [])
    
    def test_remaining_add(self):
        self._sel.set_remaining("OBJ1")
        self._sel.add_obj("OBJ2", 0.2)
        self._sel.add_obj("OBJ3", 0.4)
        
        self.assertEqual(self._sel._obj_list, ["OBJ1", "OBJ2", "OBJ3"])
        for (a, b) in zip(self._sel._prob_list, [0.4, 0.6, 1.0]):
            self.assertAlmostEquals(a,b)
        
        self.assertEqual(self._sel.get_random_obj(), "OBJ1")
        self._sel.add_obj("OBJ4", 0.2)
        self.assertEqual(self._sel.get_random_obj(), "OBJ2")
        self._rd._value=0.5
        self.assertEqual(self._sel.get_random_obj(), "OBJ3")
    
    def test_set(self):
        self._sel.set([0.2, 0.4, 0.15], ["OBJ2", "OBJ3", "OBJ4"], "OBJ1")
        self.assertEqual(self._sel._obj_list,["OBJ1", "OBJ2", "OBJ3", "OBJ4"])
        for (a, b) in zip(self._sel._prob_list, [0.25, 0.45, 0.85, 1.0]):
            self.assertAlmostEquals(a,b)
            
        self._sel.set([0.2, 0.5, 0.3], ["OBJ2", "OBJ3", "OBJ4"])
        self.assertEqual(self._sel._obj_list,["OBJ2", "OBJ3", "OBJ4"])
        for (a, b) in zip(self._sel._prob_list, [0.2, 0.7, 1.0]):
            self.assertAlmostEquals(a,b)
        
        

class FakeRandom(object):
    
    def __init__(self, value):
        self._value=value
        
    def uniform(self, a, b):
        if a<0 or b>1:
            raise ValueError("a, b should be 0 and 1 not {0}, {1}".format(
                                                                a,b))
        return self._value