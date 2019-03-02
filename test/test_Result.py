"""UNIT TESTS for the experimetn result maniupaltio

 python -m unittest test_Result
 
 
"""

from commonLib.DBManager import DB
from stats import Result, Histogram, NumericStats, NumericList

import numpy as np
import os
import unittest

class TestResult(unittest.TestCase):
    
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                       os.getenv("TEST_DB_NAME", "test"),
                       os.getenv("TEST_DB_USER", "root"),
                       os.getenv("TEST_DB_PASS", ""))
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
    def test_db(self):
        self._db.connect()
        self._db.disconnect()
        
    def test_ResultInit(self):
        res = Result("MyTable")
        self.assertEqual(res._table_name, "MyTable")
        self.assertEqual(res._data, {})
    
    def test_SetGet(self):
        res = Result("MyTable")
        res._set("MyKey1", "MyVal1")
        res._set("MyKey2", "MyVal2")
        
        self.assertEqual(res._get("MyKey1"), "MyVal1")
        self.assertEqual(res._get("MyKey2"), "MyVal2")
    
    def test_table_create(self):
        res = Result("MyTable")
        res._create_query = self.create_query
        self._table_name="MyTable"

        self.addCleanup(self._del_table, "MyTable")        
        res.create_table(self._db)
        
    def test_store_load(self):
        res = Result("MyTable", keys=["MyKey1", "MyKey2"])
        res._create_query = self.create_query
        self._table_name="MyTable"
        res._set("MyKey1", "MyVal1")
        res._set("MyKey2", "MyVal2")
        self.addCleanup(self._del_table, "MyTable")
        res.create_table(self._db)
        data_id = res.store(self._db, 1, "MyType")
        self.assertNotEqual(data_id, None)
        res = None 
        
        new_res = Result("MyTable", keys=["MyKey1", "MyKey2"])
        new_res.load(self._db, 1, "MyType")
        self.assertEqual(new_res._get("MyKey1"), "MyVal1")
        self.assertEqual(new_res._get("MyKey2"), "MyVal2")
                                 
    def create_query(self):
        return  """create table {0} (
                        id INT NOT NULL AUTO_INCREMENT,
                        trace_id INT(10) NOT NULL,
                        type VARCHAR(128) NOT NULL,
                        MyKey1 VARCHAR(100),
                        MyKey2 VARCHAR(100),
                        PRIMARY KEY(id, trace_id, type)
                    )""".format(self._table_name)
                    
                    
class TestHistogram(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                       os.getenv("TEST_DB_NAME", "test"),
                       os.getenv("TEST_DB_USER", "root"),
                       os.getenv("TEST_DB_PASS", ""))
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
        
    def test_calculate(self):
        hist = Histogram()
        
        hist.calculate([1, 2, 3, 3, 5], 1)
        bins, edges = hist.get_data()
        self.assertEqual(edges, [1, 2, 3, 4, 5, 6])
        self.assertEqual(list(bins), [0.2, 0.2, 0.4, 0, 0.2])
        
        hist.calculate([1, 2, 3, 3, 5], 1, minmax=(1,3))
        self.assertEqual(hist._get("edges"), [1, 2, 3, 4])
        self.assertEqual(list(hist._get("bins")), [0.25, 0.25, 0.5])
        
        hist.calculate([1, 2, 3, 3, 5], 1, minmax=(1,3), input_bins=[1,6]) 
        self.assertEqual(hist._get("edges"), [1, 6])
        self.assertEqual(list(hist._get("bins")), [1.0])
    
    def test_save_load(self):
        hist = Histogram()
        self.addCleanup(self._del_table, "histograms")
        hist.create_table(self._db)
        
        hist.calculate([1, 2, 3, 3, 5], 1)
        
        data_id=hist.store(self._db, 1, "MyHist")
        hist=None
        hist_new = Histogram()
        hist_new.load(self._db, 1, "MyHist")
        self.assertEqual(hist_new._get("edges"), [1, 2, 3, 4, 5, 6])
        self.assertEqual(list(hist_new._get("bins")), [0.2, 0.2, 0.4, 0, 0.2])    
    
class TestNumericStats(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                       os.getenv("TEST_DB_NAME", "test"),
                       os.getenv("TEST_DB_USER", "root"),
                       os.getenv("TEST_DB_PASS", ""))
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
        
    def test_calculate(self):
        num = NumericStats()
        
        num.calculate(range(0,101))
        data = num.get_data()
        self.assertEqual(data["count"], 101)
        self.assertEqual(data["min"], 0)
        self.assertEqual(data["max"], 100)
        self.assertEqual(data["mean"], 50)
        self.assertEqual(data["std"], np.std(range(0,101)))
        self.assertEqual(data["median"], 50)
        self.assertEqual(data["p05"], 5)
        self.assertEqual(data["p25"], 25)
        self.assertEqual(data["p50"], 50)
        self.assertEqual(data["p75"], 75)
        self.assertEqual(data["p95"], 95)
        
    
    def test_save_load(self):
        num = NumericStats()
        self.addCleanup(self._del_table, "numericStats")
        num.create_table(self._db)
        num.calculate(range(0,101))
        data_id=num.store(self._db, 1, "MyStats")
        num=None
        
        num_new = NumericStats()
        num_new.load(self._db,  1, "MyStats")
        data = num_new.get_data()
        self.assertEqual(data["count"], 101)
        self.assertEqual(data["min"], 0)
        self.assertEqual(data["max"], 100)
        self.assertEqual(data["mean"], 50)
        self.assertAlmostEqual(data["std"], np.std(range(0,101)))
        self.assertEqual(data["median"], 50)
        self.assertEqual(data["p05"], 5)
        self.assertEqual(data["p25"], 25)
        self.assertEqual(data["p50"], 50)
        self.assertEqual(data["p75"], 75)
        self.assertEqual(data["p95"], 95)

def assertEqualResult(test_obj, r_old, r_new, field):        
    d_old = r_old.get_data()
    d_new = r_new.get_data()
    if "_stats" in field:
        for (v1, v2) in zip(d_old.values(), d_new.values()):
            test_obj.assertAlmostEqual(v1,v2)
    elif "_cdf" in field:
        test_obj.assertListEqual(list(d_old[0]), list(d_new[0]))
        test_obj.assertListEqual(list(d_old[1]), list(d_new[1]))
        
class TestNumericList(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                       os.getenv("TEST_DB_NAME", "test"),
                       os.getenv("TEST_DB_USER", "root"),
                       os.getenv("TEST_DB_PASS", ""))
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
        
    def test_load_store(self):
        nl = NumericList("my_table", ["utilization", "waste"])
        self.addCleanup(self._del_table, "my_table")
        nl.create_table(self._db)
        nl.set_dic(dict(utilization=0.5, waste=100))
        nl.store(self._db, 1, "usage")
        
        nl_2 = NumericList("my_table", ["utilization", "waste"])
        nl_2.load(self._db, 1, "usage")
        self.assertEqual(nl._data, nl_2._data)
        
        
        