
from analysis.jobAnalysis import (get_jobs_data, produce_inter_times,
                                  calculate_histogram,
                                  calculate_probability_map)
from analysis import ProbabilityMap

import os


class Machine(object):
    """
    Machine class models a system and its workload to be simulated: name,
    cores per node, and job's random variables (inter arrival time, estimated
    wall clock, estimated wall clock accuracy, and allocated cores). It's
    configuration can be loaded from:
    - A database containing scheduler logs
    - Machine configuration files. 
    
    Check definition of "analysis.get_jobs_data" to understand connectivity
    requirements when pulling data from a database.    
    """
    def __init__(self, machine_name, cores_per_node=1, inter_times_filter=None,
                 num_nodes = 1):
        """
        Creation method:
        Args:
        -  machine_name: string containing the name of the system. Used to
            generate file names.
        - cores_per_node: number indication the cores present in the nodes
            of the system.
        - num_nodes: number of nodes in the system.
        """
        self._cores_per_node=cores_per_node
        self._num_nodes = num_nodes
        self._inter_times_filter=inter_times_filter
        self._machine_name=machine_name
        self._generators={}
        self._create_empty_generators()
        
    
    def load_from_db(self, start, stop, local=False, dbName="custom2"):
        """
        Connects to a database, retrieves scheduler log lines, analyze them,
        and configure the job's random variables CDFs. Check definition of
        "get_jobs_data" to understand connectivity requirements.
        Args:
        - start: number representing the epoch timestamp where the log retrieval
            should start.
        - stop: numbre representing the epoch timestamp where the log retrieval
            should stop.
        - local: if True, it will try to connect to a local MySQL database, if
            false it connects to localhost:5050 expecting it to be a tunnel
            to a remote database. 
        - dbName: Name of the MySQL database to pull the data from.
        """
        print("Loading data...")
        data_dic=get_jobs_data(self._machine_name,
                    start.year, start.month, start.day,
                    stop.year, stop.month, stop.day,
                    dbName=dbName, forceLocal = local)
        
        print("Producing inter-arrival time generator.")
        self._generators["inter"] = self._populate_inter_generator(
                                                        data_dic["created"])
        print("Producing #cores per job  generator.")
        self._generators["cores"] = self._populate_cores_generator(
                                                        data_dic["totalcores"])
        print("Producing wc_limit per job generator.")
        self._generators["wc_limit"] = (
            self._populate_wallclock_limit_generator(
                                             data_dic["wallclock_requested"]))
        print("Producing accuracy per job generator.")
        self._generators["accuracy"] = self._populate_wallclock_accuracy(
                                                data_dic["wallclock_requested"],
                                                data_dic["duration"])

    def get_inter_arrival_generator(self):
        """
        Returns the value generator for the inter arrival time random variable.
        Requires the class to be loaded.
        """
        return self._generators["inter"]
    
    def get_new_job_details(self):
        """
        Returns the number of cores, requrested wall clock, and runtime for
        a simulated job.  
        """
        cores =  self._generators["cores"].produce_number()
        wc_limit =  self._generators["wc_limit"].produce_number()
        acc =  self._generators["accuracy"].produce_number()
        run_time = int(float(wc_limit)*60*acc)
        return cores, wc_limit, run_time
        
    
    def save_to_file(self, file_dir, description):
        """
        Saves the CDFs for the random variables in 
        "[file_dir]/[description]-[machine name]-[var name].gen". Requires
        file_dir to exist
        """
        for (key, generator) in  self._generators.items():
            generator.save(self._get_files_gen(file_dir, description, key))
        
    def load_from_file(self, file_dir, description):
        """
        Loads the CDFs for the random variables from 
        "[file_dir]/[description]-[machine name]-[var name].gen".
        """
        self._create_empty_generators()
        for (key, generator) in  self._generators.items():
            generator.load(self._get_files_gen(file_dir, description, key))
        
    def _get_files_gen(self, file_dir, description, generator_name):
        return os.path.join(file_dir, "{}-{}-{}.gen".format(description, 
                                                            self._machine_name,
                                                            generator_name))
    def _create_empty_generators(self):
        
        self._generators["inter"] = self._populate_inter_generator(
                                                        [1,2])
        
        self._generators["cores"] = self._populate_cores_generator(
                                                        [self._cores_per_node])
        
        self._generators["wc_limit"] = (
            self._populate_wallclock_limit_generator([600]))
        self._generators["accuracy"] = self._populate_wallclock_accuracy([600],
                                                                         [600])
    
    def _populate_inter_generator(self, create_times):
        
        inter_times = produce_inter_times(create_times, 
                                          max_filter=self._inter_times_filter)
        
        bins, edges = calculate_histogram(inter_times, interval_size=1)
        # Any number is good, with decimals
        
        return calculate_probability_map(bins, edges,
                                         interval_policy="absnormal")
    
    def _populate_cores_generator(self, cores):
        
        bins, edges = calculate_histogram(cores, 
                                          interval_size=self._cores_per_node)
        # Always multiples of number of cores
        
        return calculate_probability_map(bins, edges,interval_policy="low",
                                         value_granularity=self._cores_per_node)
        
    def _populate_wallclock_limit_generator(self, wallclock):
        
        wallclock = [x/60 for x in wallclock]
        
        bins, edges = calculate_histogram(wallclock, interval_size=1)
        # Any number is good, with decimals
        
        return calculate_probability_map(bins, edges)
    
    def _populate_wallclock_accuracy(self, requested, actual):
        
        accuracy = []
        for (r,a) in zip(requested,actual):
            if r==0 or a==0:
                continue
            accuracy.append(float(a)/float(r))
        
        bins, edges = calculate_histogram(accuracy, interval_size=0.01)
        
        # Any number is good, with decimals
        return calculate_probability_map(bins, edges)
    def get_total_cores(self):
        """Returns the total number of cores in the system."""
        return self._num_nodes * self._cores_per_node
    def get_core_seconds_edges(self):
        """Returns the dominant groups of jobs in terms of core seconds."""
        return [0]
    def get_filter_values(self):
        """Return runtime, cores, and corehours limits for jobs in this machine.
        A job that has either more core or runtime and also more corehours than
        the values filtered should be discarded.
        """
        return None, None, None
    def get_max_interarrival(self):
        return None
    
    def job_can_be_submitted(self, cores, runtime):
        return True
class Edison(Machine):
    """Definition of machine Edison, with no CDF data for the job variables."""
    def __init__(self):
        super(Edison,self).__init__("edison", cores_per_node=24,
                                    inter_times_filter=1800,
                                    num_nodes=5576)
        
class Edison2015(Machine):
    """Edison machine that loads the CDF values of 2015 for the the job
    variables."""
    def __init__(self):
        super(Edison2015,self).__init__("edison", cores_per_node=24,
                                    inter_times_filter=1800,
                                    num_nodes=5576)
        self.load_from_file("./data", "2015")
    
    def get_max_interarrival(self):
        return 25
    
    def job_can_be_submitted(self, cores, runtime):
        if (cores>self.get_total_cores()/4):
            return False
        if (runtime>24*3600*5):
            return False
        if ((cores>15000 or runtime>15000) 
            and runtime*cores>10000*10000):
            return False
        return True
    def get_core_seconds_edges(self):
        """Returns the dominant groups of jobs in terms of core seconds."""
        return [0, 48*3600, 960*3600]
