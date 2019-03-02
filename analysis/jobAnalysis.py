"""
Set of functions to analysis job related in HPC workloads. 
"""

from analysis import ProbabilityMap
from commonLib.nerscLib import (getDBInfo, parseFromSQL_LowMem, 
                                getSelectedDataFromRows)

from slurm.trace_gen import extract_records 

 
import numpy as np

def get_jobs_data_trace(file_name, list_trace_location="./list_trace"):
    """
    Parses a trace file and returns a dictionary with lists of the jobs'
    duration, allocated cores, wallclock requested and created epoch timestamp.
    Args:
        file_name: trace file to load.
        list_trace_location: location of slurm simulator list_trace
        command.
    Returns: a dictionary with the keys "duration","totalcores",
        "wallclock_requested", "created".
    """
    jobs = extract_records(file_name, list_trace_location)  
    data_dic = dict(duration=[], totalcores=[], wallclock_requested=[],
                    created=[])
    for job in jobs:
        data_dic["duration"].append(int(job["DURATION"]))
        data_dic["totalcores"].append(job["NUM_TASKS"]*job["CORES_PER_TASK"])
        data_dic["wallclock_requested"].append(int(job["WCLIMIT"]))
        data_dic["created"].append(int(job["SUBMIT"]))
    return data_dic


def get_jobs_data(hostname, startYear, startMonth, startDay, stopYear, 
                  stopMonth, stopDay, dbName="custom2", forceLocal = False):
    """
    Retrieves PBS log dat info from a MySQL database and returns a set of
    lists of job atrributes. Same position in different lists are
    associated with the same job.  It uses NERSCDB_USER and
    NERSCDB_PASS as username, password to connect.
    
    Args:
        hostname: string with the name of the system which logs.
        startYear, startMonth, startDay: numbers representing the start
            point of the log retrieval.
        stopYear, stopMonth, stopDay: numbers representing the end point
            of the log retrieval.
        dbName: Name of the database containing the data.
        forceLocal: if true, forces connection to localhost, if False
            it connects to localhost:5050 expecting to connect through a
            tunnel.
    Returns: a dictionary with four components, each one contaning a list
        of values. All lists have the same number of elements and the
        same position in different lists refer to the same job. Components:
            - duration: wall clock time of the jobs.
            - totalcores: cores allocated by each job.
            - wallclock_request: runtime limit for each job.
            - created: epoch timestamp of the job submission.
                
    """
    info=getDBInfo(forceLocal)
    if (info!=None):
        (dbHost, user, password, dbPort)=info
    else:
        print("Error retrieving data to connect to DB")
    rows, start=parseFromSQL_LowMem(
        dbName=dbName, hostname=hostname, dbHost=dbHost, dbPort=dbPort,
        userName=user, password=password, 
        year=startYear, month=startMonth, day=startDay,  
        endYear=stopYear, endMonth=stopMonth, endDay=stopDay, 
        orderingField="created")
    dataFields=["duration","totalcores", "wallclock_requested", "created"]
    classFields=[]

    (numberSamples, outputDic, outputAcc, 
     queues, queuesDic, queuesG, queueGDic) = getSelectedDataFromRows(rows,
                                                    dataFields, classFields)
    print("Retrieved {0} records from database {1} at {2}:{3}".format(
        numberSamples, dbName, dbHost, dbPort))
    return outputDic
def _filter_data(self, series, limitMax):
        new_series = []
        for x in series:
            if x <= limitMax:
                new_series.append(x)
        return new_series
    
def produce_inter_times(timestamps, max_filter=None):
    """
    Produces a list of the ordered timespans in seconds between epoch
    timestamps.
    
    Args:
        timestamps: list of positive numbers representing epoch time
            stamps in seconds.
        
    Returns:
        inter_times: list of time invervals (in seconds) between the
            values in timestamps.
    """
    inter_times  = []
    last_t=None
    for t in timestamps:
        if (not last_t is None) and (not t is None):
            if t<last_t:
                raise ValueError("Timestamps are not increasing!")
            if t!=0 and last_t!=0:
                if max_filter is None or t-last_t<=max_filter:
                    inter_times.append((t-last_t))
        last_t=t
    return inter_times


def _join_var_bins(hist, bin_edges, th_min=0.01, th_acc=0.1):
    """ 
    Joins adjacent bins in a normalized histogram if: (1) they contribute less
    than th_min share, (2) while the new aggregated bin does not contribute
    over th_acc share.
    
    Args:
        hist: normalized histogram exprssed as a enumerable of [0, 1] shares.
            All elements must sum 1.0
        bin_edges: edges of the histogram. contains one more element than hist.
        th_min: maximum size of a bin to be fused with a adjacent bin (that bin
            hast to be also < th_min)
        th_acc: maximum size of the resulting bin after fusing bins.
    Returns:
        composed_hist: histogram after fusing bins (narray).
        composed_edges: corresponding edges. Contains one more element than
            composed_hist.    
    """
    composed_hist=[]
    composed_edges=[]

    max_edge = None
    composed_edges.append(bin_edges[0])
    acc=0
    for (share, edge) in zip(hist, zip(bin_edges[:-1], bin_edges[1:])):
        if (share>th_min or acc+share>th_acc):
            if max_edge!=None:
                composed_hist.append(acc)
                composed_edges.append(max_edge)  
                acc=0
            composed_hist.append(share)
            composed_edges.append(edge[1])  
            max_edge=None  
        else:
            acc+=share
            max_edge=edge[1]
    if max_edge!=None:
        composed_hist.append(acc)
        composed_edges.append(max_edge)  
    hist = np.array(composed_hist, dtype=float)
    bin_edges = composed_edges
    return hist, bin_edges

def calculate_probability_map(hist, bin_edges,**kwargs):
    """ 
    Transforms a histogram into a probability map. The map represents the
        chance for a random variable to take a value in a particular range.
        It eliminates intervals with 0 probability of happening.
    Args:
        hist: value list of a histogram. It does not have to be uniform, but
            its components have to sum 1.0
        bin_edges: edges of the histogram. The interval do not have to be
            all the same.
        **kwargs: Arguments that the ProbabilityMap constructor accepts
    Returns:
        prob: ordered list of float, CDF of the probability of each value
            interval.
        values: list of intervals [a,b) as tuples. Represent interval of numbers
            associated with each probability.
    """
    prob_values = []
    value_range = []
    acc=0
    for (share, edge) in zip(hist, zip(bin_edges[:-1], bin_edges[1:])):
        if (share!=0):
            acc+=share
            prob_values.append(acc)
            value_range.append(edge)
    return ProbabilityMap(probabilities=prob_values, 
                          value_ranges=value_range, **kwargs)
    
def calculate_histogram(data, th_min=0, 
                        th_acc=1, range_values=None, interval_size=1,
                        total_count=None, bins=None):
    """
    Produces a non normalized histogram, with non uniform intervals. It joins
        too small adjacent bins, but limiting the size of the joint bins.
    Args:
        data: list of values to do the histogram on.
        th_min: resulting adjacent bins < th_min will be joined. 
        th_acc: Joined bins, won't be bigger than th_acc.
        range_values: tuple expressing the min, max values to do the histogram.
            if not set, it is (min(data), max(data))
        interval_size:  width of the resulting bins.
        total_count: count of values to use the normalization, e.g. the data
            might be a subset of the total data to do the histogram for. If
            not set it is len(data).
    Returns:
        hist: narray of float values representing the share of each bin.
        bin_edges: edges of each bin as a list of numbres. It has one
            more element than hist.
        
    """
    if range_values is None:
        range_values=(min(data), max(data))
    range_values=(range_values[0], range_values[1]+interval_size)
    if bins is None:
        bins= np.arange(range_values[0], range_values[1]+interval_size,
                        interval_size)

    hist_count, bin_edges = np.histogram(data, 
                                         density=False, range=range_values,
                                         bins=bins)
    if total_count is None:
        total_count = sum(hist_count)
    hist = np.array(hist_count)/float(total_count)
    
    hist, bin_edges = _join_var_bins(hist, bin_edges, th_min=th_min, th_acc=th_acc)
    return hist, bin_edges




        