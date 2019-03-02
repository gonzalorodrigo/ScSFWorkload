from analysis.jobAnalysis import calculate_histogram
import cPickle
import MySQLdb
import numpy as np


class Result(object):
    """
    Abstract class for results on a workload analysis. Such a class has to be
    able to calculate some data over a data set. This class stores the results
    in an database, data can also be retrieved from a DB. 
    
    Should expose data in a way that can be plotted.
    """
    
    def __init__(self, table_name, keys=None):
        """ Constructor 
        Args:
        - table_name: string containing the database table name that
            store data from this Result.
        - keys: list of unique strings identifying each component of a result.
        """
        if keys is None:
            keys=[]
        self._data = {}
        self._table_name=table_name
        self._keys= keys
        
    def calculate(self, dataset):    
        """
        Calculate's statistics on dataset and stores results in self._data.
        """
        pass
    
    def store(self, db_obj, trace_id, measurement_type):
        """
        Stores the content of self._data in the self._table_name table. 
        Args:
        - db_obj: Data DBManager object to interact with database.
        
        Returns: the primary key identifying the result entry in the
        database. 
        
        Raises SystemError exception if insertiln fails. 
        """
        keys  = self._data.keys()
        values = [self._encode(self._data[key], key) for key in keys]
        keys = ["trace_id", "type"] + keys
        values= [trace_id, measurement_type] + values
        ok, insert_id = db_obj.insertValues(self._table_name, keys, values,
                                            get_insert_id=True)
        if not ok:
            raise SystemError("Data insertion failed")
        return insert_id
    
    def load(self, db_obj, trace_id, measurement_type):
        """ 
        Loads self._data from the row indentified by data_id in self._table_name
        table.
        Args:
        - db_obj: DBManager object allows access to a database.
        - data_id: id of the record to load for the database.
        """
        keys  = self._keys
        data_dic=db_obj.getValuesDicList(self._table_name, keys, condition=
                                        "trace_id={0} and type='{1}'".format(
                                        trace_id, measurement_type))
        if data_dic is not None and data_dic != ():
            for key in keys:
                self._set(key, self._decode(data_dic[0][key], key))
    
    def get_data(self):
        return self._data
    
    def _set(self, data_name, data_value):
        self._data[data_name] = data_value
    
    def _get(self, data_name):
        if data_name in self._data.keys():
            return self._data[data_name]
        return None
    
    def _encode(self, data_value, key):
        """
        Encodes data_value to the format of a column of the table used by
        this class. To be re-implemented in child classes as the table
        defintion will change."""
        return data_value
    def _decode(self, blob, key):
        """
        Decodes blob from the format outputed by a dabatase query. To be
        re-implemented in child classes as the table implementation will
        change."""
        return blob 

    def create_table(self, db_obj):
        """
        Creates the table associated with this Result class.
        Args:
        - db_obj: DBManager object allows access to a database.
        """
        db_obj.doUpdate(self._create_query())
    
    def _create_query(self):
        """Returns a string with the query needed to create a table
        corresponding to this Result class. To be modifed according to the table
        formats required by the child classes."""
        return  ""
    
    def get_list_of_results(self, db_obj, trace_id):
        """Returns a list of the result types corresponding to this Result that
        are for a trace identified by trace_id.
        Args:
        - db_obj: DBMaster connected object.
        - trace_id: integer id of a trace
        """
        lists = db_obj.getValuesAsColumns(
                self._table_name, ["type"], 
                condition = "trace_id={0}".format(trace_id))
        return lists["type"]
    
    def plot(self, file_name):
        """Plots results on a filename"""
        pass
    
class Histogram(Result):
    """
    Histogram Result class. It produces an histogram (bins and edges) on a
    dataset.
    """
    def __init__(self):
        super(Histogram,self).__init__(table_name="histograms",
                                       keys = ["bins", "edges"])
    
    def calculate(self, data_set, bin_size, minmax=None, input_bins=None):
        """
        Calculates the histogram according to the data_set.
        Args:
        - data_set: list of numbers to be analyzed.
        - bin_size: float pointing to the size of the output bins.
        - minmax: tuple (min, max) numbers to perform the histogram over.
        - input_bins: list of edges to be used in the histogram. if set it
        overrides bin_size.
        """
        if bin_size is None and minmax is None:
            raise ValueError("Either bin_size or bin has to be set")
        bins, edges  = calculate_histogram(data_set, th_min=0.0, th_acc=0.0,
                                               range_values=minmax, 
                                               interval_size=bin_size,
                                               bins=input_bins)
         
        self._set("bins", bins)
        self._set("edges", edges)
        
    def get_data(self):
        return self._get("bins"), self._get("edges")
    
    def _create_query(self):
        return """create table {0} (
                    id INT NOT NULL AUTO_INCREMENT,
                    trace_id INT(10) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    bins LONGBLOB,
                    edges LONGBLOB,
                    PRIMARY KEY(id, trace_id, type)
                )""".format(self._table_name)
    
    def _encode(self, data_value, key):
        """Datbase uses blobls to store the edges and bins"""
        pickle_data = cPickle.dumps(data_value)
        return MySQLdb.escape_string(pickle_data)
    def _decode(self, blob, key):
        return cPickle.loads(blob)

class NumericList(Result):
    
    def _create_query(self):
        cad= """create table `{0}` (
                    id INT NOT NULL AUTO_INCREMENT,
                    trace_id INT(10) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    """.format(self._table_name)
        for field in self._keys:
            cad+=" {0} DOUBLE,".format(field)
        cad+="""   PRIMARY KEY(id, trace_id, type))"""
        return cad

    def set_dic(self, the_dic):
        for (key,value) in the_dic.iteritems():
            self._set(key, value)
    
    def apply_factor(self, factor):
        for key in self._keys:
            self._set(key, float(self._get(key))*float(factor))
            
class NumericStats(Result):
    """
    Does a basic analysis over a dataset including: minimum, maximum, mean, 
    standard deviation, dataset count, median and five percentiles (5, 25, 50
    75,95).
    
    Returned object by get_data is a dictionary indexed by these keys: "min",
    "max", "mean", "std", "count", "median", "p05", "p25", "p50", "p75", "p95".
    """
    def __init__(self):
        super(NumericStats,self).__init__(table_name="numericStats",
            keys = ["min", "max", "mean", "std", "count", "median",
                    "p05", "p25", "p50", "p75", "p95" ])
    
    def apply_factor(self, factor):
        for key in ["min", "max", "mean", "std", "median",
                    "p05", "p25", "p50", "p75", "p95" ]:
            self._set(key, float(self._get(key))*float(factor))
            
    def calculate(self, data_set):
        """Calculates a number of numerica statistical metrics over the numbers
        in the data_Set list.
        """
        x = np.array(data_set, dtype=np.float)
        self._set("min", min(x))
        self._set("max", max(x))
        self._set("mean", np.mean(x))
        self._set("std", np.std(x))
        self._set("count", x.shape[0])
        
        percentile_name=["p05", "p25", "p50", "p75", "p95"]
        percentlie_values = np.percentile(x, [5, 25, 50, 75, 95])
        self._set("median", percentlie_values[2])
        for (key, per) in zip(percentile_name, percentlie_values):
            self._set(key, per)
        

    def _encode(self, data_value, key):
        return data_value
    def _decode(self, blob, key):
        return float(blob) 
    
    def _create_query(self):
        return """create table {0} (
                    id INT NOT NULL AUTO_INCREMENT,
                    trace_id INT(10) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    min DOUBLE,
                    max DOUBLE,
                    mean DOUBLE,
                    std DOUBLE,
                    count int, 
                    median DOUBLE,
                    p05 DOUBLE,
                    p25 DOUBLE,
                    p50 DOUBLE,
                    p75 DOUBLE,
                    p95 DOUBLE,
                    PRIMARY KEY(id, trace_id, type)
                )""".format(self._table_name)
    
    def get_values_boxplot(self):
        data_names = "median", "p25", "p75", "min", "max" 
        return [self._get(x) for x in data_names]
    
    
def calculate_results(data_list, field_list, bin_size_list,
                      minmax_list, store=False, db_obj=None, trace_id=None):
    """Calculates CDF and Stats result over the lists of values in data_list.
    Sets the results as variables of caller_obj. 
    Args: 
    - caller_obj: Object over which the results will be set as 
        "_[field_name]_cdf" for Histogram objects and "_[field_name]_stats"
        for NumericStats objects.
    - data_list: lists of lists of values to be analyzed.
    - field_list: list of strings with the name of the data set in the same
        position as in data_list.
    - bin_size_list:  list of Bin sizes to be used for the CDF analysis of the
        dataset in the same position at data_list.
    - minmax_list: lists if tuples of numbers with the maximum and minimum
        values to use in each CDF analysis.
    - store: if True, the resulting Result objects will store their content
        in a database.
    - db_obj: DBManager object configured to access a database on which data
        was stored.
    - trace_id: Numeric ID of the trace originating the data in data_list.    
    """

    cdf_field_list = [x+"_cdf" for x in field_list]
    stats_field_list = [x+"_stats" for x in field_list]
    results_dic={}
    for (data, cdf_field, stats_field, bin_size, minmax) in zip(data_list,
            cdf_field_list, stats_field_list, bin_size_list, minmax_list):
        if data:
            cdf = Histogram()
            cdf.calculate(data, bin_size=bin_size, minmax=minmax)
            if store:
                cdf.store(db_obj, trace_id, cdf_field)
            results_dic[cdf_field]=cdf
        
        stats = NumericStats()
        if data:
            stats.calculate(data)
            if store:
                stats.store(db_obj, trace_id, stats_field)
            results_dic[stats_field]=stats
    return results_dic

def load_results(field_list, db_obj, trace_id):
    """Creates a number of Histogram and NumericStats objects, populate from
    the database and set them as variables of caller_obj.
    Args:
    - caller_obj: Object where the result objects will be set.
    - field_list: type of data the the results are loaded. Corresponding results
        will be pulled for Histogram and NumericStats tables.
    - db_obj: DBManager object configured to access a database from which data
        will be retrieved.
    - trace_id: numeric id identifying the trace to which the data should be
        loaded.
    """
    results = {}
    cdf_field_list = [x+"_cdf" for x in field_list]
    stats_field_list = [x+"_stats" for x in field_list]
    for (cdf_field, stats_field) in zip(cdf_field_list, stats_field_list):
        cdf = Histogram()
        cdf.load(db_obj, trace_id, cdf_field)
        if cdf is not None:
            results[cdf_field] = cdf
        
        stats = NumericStats()
        stats.load(db_obj, trace_id, stats_field)
        if stats is not None:
            results[stats_field] = stats
    return results
        
        
    
    
