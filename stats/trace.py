"""
This package incldues a number of classes to import and manipulate
scheduling log traces.
"""
import numpy as np

from stats import (calculate_results, load_results, NumericList)
from stats.workflow import WorkflowsExtractor
from commonLib.nerscUtilization import UtilizationEngine

class ResultTrace(object):
    """ This class stores scheduling simulation result traces. It is designed to
    import such traces from slurm's accounting MySQL database and store them in
    a different database. It can be also populated from a copy in that second
    database.
    
    It also processes the traces, retrieving the values required for the trace
    analysis, and running the corresponding analysis. The results can also be
    stored in the corresponding database (also loaded).
    
    The required database table for the trace storage is described in
    create_trace_table. 
    """
    def __init__(self, table_name="traces"):
        """Constructor
        Args:
        - table_name: table where traces should be stored.  
        """
        self._lists_submit = {}
        self._lists_start = {}
        self._table_name=table_name
        self._fields= ["job_db_inx", "account", "cpus_req", "cpus_alloc",
          "job_name", "id_job", "id_qos", "id_resv", "id_user", 
          "nodes_alloc", "partition", "priority", "state", "timelimit",
          "time_submit", "time_start", "time_end"]
        self._wf_extractor = None
        self._integrated_ut = None
        self._acc_waste = None
        self._corrected_integrated_ut = None
    def _clean_db_duplicates(self, db_obj, table_name):
        query="""SELECT id_job, dup, inx from 
              (SELECT `id_job`,count(*) dup, max(job_db_inx) inx 
               FROM {0}  GROUP BY id_job) as grouped
              WHERE dup>1""".format(table_name)
        duplicates = db_obj.getValuesAsColumns(
                            table_name, ["id_job", "dup", "inx"],
                            theQuery=query)
        print "Cleaning duplicated entries"
        print "Duplicated entries before:", len(duplicates["id_job"])
        for (id_job, job_db_inx) in zip(duplicates["id_job"],duplicates["inx"]):
            query = """DELETE FROM `{0}` 
                       WHERE `id_job`={1} and `job_db_inx`!={2}
            """.format(table_name, id_job,job_db_inx)
            db_obj.doUpdate(query)
        duplicates = db_obj.getValuesAsColumns(
                            table_name, ["id_job", "dup", "inx"],
                            theQuery=query)
        print "Duplicated entries after:", len(duplicates["id_job"])
        
    
    def import_from_db(self, db_obj, table_name, start=None, end=None):
        """ Imports a scheduler simulation trace from a database. 
        Args:
        - db_obj: DBManager object configured to connect to a slurm accounting
            database.
        - table_name: name of the job table in the database. Expected format
            of the table can be found at create_import_table
        - start: If set to an epoch date, it will retrieve jobs created
            after start.
        - end: if set to an epoch time, it will retrieve jobs before start.
        """
        self._clean_db_duplicates(db_obj, table_name)
        self._lists_submit = db_obj.getValuesAsColumns(
                            table_name, self._fields, 
                            condition = _get_limit("time_submit",start, end),
                            orderBy="time_submit")
        
        self._lists_start = db_obj.getValuesAsColumns(
                            table_name, self._fields, 
                            condition = _get_limit("time_start",start, end),
                            orderBy="time_start")
    
    def import_from_pbs_db(self, db_obj, table_name, start=None, end=None,
                           machine=None):
        """ Imports a torque/moab NERSC style workload trace from a database 
        Args:
        - db_obj: DBManager object configured to connect to a pbs style
            database.
        - table_name: name of the job table in the database. Expected format
            of the table can be found at create_import_table
        - start: If set to an epoch date, it will retrieve jobs created
            after start.
        - end: if set to an epoch time, it will retrieve jobs before start.
        """
        pbs_fields = ["account", "jobname", "cores_per_node", "numnodes", 
         "class", "wallclock_requested", "created", "start", "completion"]
        machine_cond=""
        if machine:
            machine_cond=" and `hostname`='{0}'".format(machine)
        self._lists_submit = self._transform_pbs_to_slurm(
                                db_obj.getValuesAsColumns(
                                    table_name, pbs_fields, 
                                    condition = (_get_limit("created",start,end)
                                                 +machine_cond),
                                    orderBy="created"))
                                                    
        
        self._lists_start = self._transform_pbs_to_slurm(
                                db_obj.getValuesAsColumns(
                                    table_name, pbs_fields, 
                                    condition = (_get_limit("created",start,end)
                                                 +machine_cond),
                                    orderBy="start"))
    
    
    def _transform_pbs_to_slurm(self, pbs_list):
        """ Returns a slurm style job list from the pbs_list."""
        slurm_list = {}
        for field in self._fields:
            slurm_list[field] = []
        job_count = len(pbs_list.values()[0])
        slurm_list["account"] = pbs_list["account"]
        slurm_list["job_name"] = pbs_list["jobname"]
        slurm_list["partition"] = pbs_list["class"]
        slurm_list["time_submit"] = pbs_list["created"]
        slurm_list["time_start"] = pbs_list["start"]
        slurm_list["time_end"] = pbs_list["completion"]
        slurm_list["nodes_alloc"] = pbs_list["numnodes"]
        slurm_list["timelimit"] = [x/60 for x in 
                                    pbs_list["wallclock_requested"]]
        fake_ids=[3 for x in pbs_list["wallclock_requested"]]
        slurm_list["id_qos"] =  fake_ids
        slurm_list["id_resv"] =  fake_ids
        slurm_list["id_user"] =  fake_ids
        slurm_list["priority"] =  fake_ids
        slurm_list["state"] =  fake_ids
        
        slurm_list["cpus_req"] = [x*y for (x,y) in zip(pbs_list["numnodes"],
                                                    pbs_list["cores_per_node"])]
        slurm_list["cpus_alloc"] = slurm_list["cpus_req"]
        slurm_list["job_db_inx"] = range(job_count)
        slurm_list["id_job"] = slurm_list["job_db_inx"] 
        
        return slurm_list
                
    def store_trace(self, db_obj, trace_name):
        """ Stores the trace in a database.
        Args:
        - db_obj: DBManager object configured to connect to a database which
            hosts a table named self._table_name with the structure defined
            in create_trace_table
        - tarce_name: string containing the unique ID of the trace.
        """
        db_obj.insertValuesColumns(self._table_name,
                                   self._lists_submit,
                                   {"trace_id":trace_name})
    
    
    def load_trace(self, db_obj, trace_id, append=False):
        """ Retrieves a trace from a database
        Args:
        - db_obj: DBManager object configured to connect to a database which
            hosts a table named self._table_name with the structure defined
            in create_trace_table
        - tarace_name: string containing the unique ID of the trace to
            be retrieved.
        - append: if True, this trace is added to the one in the object,
            otherwise the content of the class is overwritten. Also, time
            stamps of the added trace are recalculated, as the newly loaded
            trace happened just after the previously loaded ones. 
        """
        if not append:
            self._lists_submit = {}
            self._lists_start = {}
            self._load_trace_count = 0
            time_offset=0
        else:
            self._load_trace_count += 1
            time_offset = self._lists_submit["time_submit"][-1]
        
        new_lists_submit= db_obj.getValuesAsColumns(
                              self._table_name, self._fields, 
                              condition = "trace_id={0}".format(trace_id),
                              orderBy="time_submit")
        first_time_value=new_lists_submit["time_submit"][0]
        ResultTrace.apply_offset_trace(new_lists_submit, time_offset,
                                       first_time_value)
        self._lists_submit= ResultTrace.join_dics_of_lists(
                            self._lists_submit,new_lists_submit)
        
        new_lists_start = db_obj.getValuesAsColumns(
                              self._table_name, self._fields, 
                              condition = "trace_id={0}".format(trace_id),
                              orderBy="time_start")
        
        ResultTrace.apply_offset_trace(new_lists_start, time_offset,
                                       first_time_value)
        self._lists_start= ResultTrace.join_dics_of_lists(
                            self._lists_start,
                            new_lists_start)
    @classmethod
    def apply_offset_trace(cls, lists, offset=0, first_time_value=0,
                           time_fields=["time_start", "time_end","time_submit"]
                           ):
        """Applies an offest to all the time_fileds of a trace.
        Args:
        - lists: dictionary of lists of trace values (e.g. Trace._lists_submit)
        - offset: value in seconds to apply to the time values.
        - first_time_value: seconds to be substracted from all time values.
        - time_files: list of stings naming the time fields to be re-calculated. 
        """
        if offset != 0:
            offset+=1
            for field in time_fields:
                lists[field] =[x+offset-first_time_value for x in lists[field]] 
    
    @classmethod   
    def join_dics_of_lists(self, dic1, dic2):
        """ Returns a new dictionary: joins two dictionaries of lists """
        new_dic = {}
        keys = dic1.keys()+dic2.keys()
        keys = list(set(keys))
        for key in keys:
            new_dic[key]=[]
            if key in dic1.keys():
                new_dic[key]+=dic1[key]
            if key in dic2.keys():
                new_dic[key]+=dic2[key]
        return new_dic
        
    def analyze_trace(self, store=False, db_obj=None, trace_id=None):
        """Calculates and stores stats on job and workflow variables.
        Args:
        - store: if true, stats are stores in the database.
        - db_obj: DB object configured to access an analysis database where the
            results will be stores.
        - trace_id: interger id of the trace to store the results as.
        """
        self.calculate_job_results(store=store, db_obj=db_obj, 
                                   trace_id=trace_id)
        self.calculate_workflow_results(store=store, db_obj=db_obj, 
                                   trace_id=trace_id)

    def load_analysis(self,  db_obj, trace_id, core_seconds_edges=None):
        """Loads job and workflow results for a trace trace_id"""
        self.load_job_results(db_obj, trace_id)
        self.load_workflow_results(db_obj, trace_id)
        if core_seconds_edges:
            self.load_job_results_grouped_core_seconds(core_seconds_edges,
                                                       db_obj, trace_id)
        
    def load_job_results(self, db_obj, trace_id):
        """ Creates Histogram and NumericStats objects and sets them as local
        object as self._[analyzed job field] over the information of the jobs
        in a trace. The information is pull from a database.
        Args:
        - db_obj: DBManager Object used to pull the information from a database.
        - trace_id: numeric id of the trace to which the data corresponds.
        """
        field_list=["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                    "jobs_requested_wc", "jobs_cpus_alloc", "jobs_slowdown"]
        self.jobs_results= load_results(field_list, db_obj, trace_id)
    
    def load_job_results_grouped_core_seconds(self, core_seconds_edges,
                                              db_obj, trace_id):
        """ Creates Histogram and NumericStats objects and sets them as local
        object as self._g[edge]_[analyzed job field] over the information of the
        jobs in a trace, grouped by core seconds. The information is pulled
        from a database.
        Args:
        - core_seconds_edges: list of edges for the groups jobs by allocated
            cores seconds
        - db_obj: DBManager Object used to pull the information from a database.
        - trace_id: numeric id of the trace to which the data corresponds.
        """
        new_fields=[]
        for edge in core_seconds_edges:
            field_list=["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                        "jobs_requested_wc", "jobs_cpus_alloc",
                        "jobs_slowdown"]
            new_fields+=[ResultTrace.get_result_type_edge(edge,x)
                          for x in field_list]
        if not hasattr(self, "job_results"):
            self.jobs_results={}
        self.jobs_results.update(load_results(new_fields, db_obj, trace_id))
    
    def get_grouped_result(self, core_seconds_edge, result_type):
        key=ResultTrace.get_result_type_edge(core_seconds_edge, result_type)
        if key in self.jobs_results.keys():
            return self.jobs_results[key]
        return None

    @classmethod   
    def get_result_type_edge(cld, core_seconds_edge, result_type):
        return "g"+str(core_seconds_edge)+"_"+result_type
        
    
    def load_workflow_results(self, db_obj, trace_id):
        """ Creates Histogram and NumericStats objects and sets them as local
        object as self._[analyzed job field] over the information of the
        workflows in a trace: all the workflows and per manifest present in the
        workload. The information is pull from a database.
        Args:
        - db_obj: DBManager Object used to pull the information from a database.
        - trace_id: numeric id of the trace to which the data corresponds.
        """
        self._wf_extractor = WorkflowsExtractor()
        self.workflow_results=self._wf_extractor.load_overall_results(db_obj, trace_id)
        self.workflow_results_per_manifest \
                =self._wf_extractor.load_per_manifest_results(db_obj, trace_id)
        
    
    def _get_job_times(self, submit_start=None, submit_stop=None, 
                       only_non_wf=False):
        """ Returns runtime, wait time and turnaround time of jobs submitted
        between submit_start and submit_stop
        Args:
        - submit_start: integer epoch date. If set, data of job's submitted
            before this is not returned. 
        - submit_stop: integer epoch date. If set, data of job's submitted
            after this is not returned.
        - only_non_wf: boolean, if True, data from jobs belonging to workflows
            are not returned. 
        Returns three lists containing jobs_runtime, jobs_waittime,
            jobs_turnaround, jobs_slowdown
        """
        jobs_runtime = []
        jobs_waittime = []
        jobs_turnaround = []
        jobs_timelimit= []
        jobs_cores_alloc = []
        jobs_slowdown = []
        for (end, start, submit, time_limit, cpus_alloc, job_name) in zip(
                                        self._lists_submit["time_end"], 
                                        self._lists_submit["time_start"],
                                        self._lists_submit["time_submit"],
                                        self._lists_submit["timelimit"],
                                        self._lists_submit["cpus_alloc"],
                                        self._lists_submit["job_name"]):
            if (end == 0 or start==0 or submit==0 or end<start or start<submit
                or (start==end)
                or (submit_start is not None and submit<submit_start) 
                or (submit_stop is not None and submit>submit_stop)):
                #print "discarded!", submit_start, submit_stop, submit, start, end
                continue
            if (only_non_wf and len(job_name)>=3 and job_name[0:3]=="wf_"):
                continue
            jobs_runtime.append(end-start)
            jobs_waittime.append(start-submit)
            jobs_turnaround.append(end-submit)
            jobs_timelimit.append(time_limit)
            jobs_cores_alloc.append(cpus_alloc)
            jobs_slowdown.append(float(end-submit)/float(end-start))
        return (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown)
    
    def get_job_times_grouped_core_seconds(self,
                       core_seconds_edges,
                       submit_start=None, submit_stop=None, 
                       only_non_wf=False):
        """ Returns runtime, wait time and turnaround time of jobs submitted
        between submit_start and submit_stop, grouped by jobs with similar
        core seconds. 
        Args:
        - cores_econds_edges: list of ints with the core seconds limits for the
            job grouping. i_0: [i0, i_1], i_1: (i_1, i_2] ...
            i_n-1: (i_n-1, infinity] 
        - submit_start: integer epoch date. If set, data of job's submitted
            before this is not returned. 
        - submit_stop: integer epoch date. If set, data of job's submitted
            after this is not returned.
        - only_non_wf: boolean, if True, data from jobs belonging to workflows
            are not returned. 
        Returns three dictionaries of lists containing jobs_runtime,
            jobs_waittime, jobs_turnaround, jobs_slowdown. Each dic is indexed
            by the values in core_seconds_edges.
        """
        jobs_runtime = {}
        jobs_waittime = {}
        jobs_turnaround = {}
        jobs_timelimit= {}
        jobs_cores_alloc = {}
        jobs_slowdown = {}
        jobs_timesubmit = {}
        for edge in core_seconds_edges:
            jobs_runtime[edge] = []
            jobs_waittime[edge] = []
            jobs_turnaround[edge] = []
            jobs_timelimit[edge] = []
            jobs_cores_alloc[edge] = []
            jobs_slowdown[edge] = []
            jobs_timesubmit[edge] = []
        
        for (end, start, submit, time_limit, cpus_alloc, job_name,
             time_submit) in zip(
                                        self._lists_submit["time_end"], 
                                        self._lists_submit["time_start"],
                                        self._lists_submit["time_submit"],
                                        self._lists_submit["timelimit"],
                                        self._lists_submit["cpus_alloc"],
                                        self._lists_submit["job_name"],
                                        self._lists_submit["time_submit"]):
            if (end == 0 or start==0 or submit==0 or end<start or start<submit
                or (start==end)
                or (submit_start is not None and submit<submit_start) 
                or (submit_stop is not None and submit>submit_stop)):
                #print "discarded!", submit_start, submit_stop, submit, start, end
                continue
            if (only_non_wf and len(job_name)>=3 and job_name[0:3]=="wf_"):
                continue
            edge = self._get_index_in_core_seconds_list(time_limit*60,
                                                        cpus_alloc,
                                                        core_seconds_edges)
            jobs_runtime[edge].append(end-start)
            jobs_waittime[edge].append(start-submit)
            jobs_turnaround[edge].append(end-submit)
            jobs_timelimit[edge].append(time_limit)
            jobs_cores_alloc[edge].append(cpus_alloc)
            jobs_slowdown[edge].append(float(end-submit)/float(end-start))
            jobs_timesubmit[edge].append(time_submit)
        return (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown, jobs_timesubmit)
        
    def get_job_values_grouped_core_seconds(self,
                       core_seconds_edges,
                       submit_start=None, submit_stop=None, 
                       only_non_wf=False,
                       fields=["time_submit", "time_start"]):
        """ Returns runtime, wait time and turnaround time of jobs submitted
        between submit_start and submit_stop, grouped by jobs with similar
        core seconds. 
        Args:
        - cores_econds_edges: list of ints with the core seconds limits for the
            job grouping. i_0: [i0, i_1], i_1: (i_1, i_2] ...
            i_n-1: (i_n-1, infinity] 
        - submit_start: integer epoch date. If set, data of job's submitted
            before this is not returned. 
        - submit_stop: integer epoch date. If set, data of job's submitted
            after this is not returned.
        - only_non_wf: boolean, if True, data from jobs belonging to workflows
            are not returned. 
        Returns three dictionaries of lists containing jobs_runtime,
            jobs_waittime, jobs_turnaround, jobs_slowdown. Each dic is indexed
            by the values in core_seconds_edges.
        """
        jobs_dic={}
        
        for field in fields:
            jobs_dic[field]={}
            for edge in core_seconds_edges:
                jobs_dic[field][edge] = []

        for (timelimit, cpus_alloc, i) in zip(
                            self._lists_submit["timelimit"],
                            self._lists_submit["cpus_alloc"],
                            range(len(self._lists_submit["timelimit"]))):
            edge = self._get_index_in_core_seconds_list(timelimit*60,
                                                        cpus_alloc,
                                                        core_seconds_edges)
            for field in fields:
                jobs_dic[field][edge].append(self._lists_submit[field][i])
            
        return jobs_dic
    def _get_index_in_core_seconds_list(self,runtime, cpus_alloc, 
                                        core_seconds_edges):
        core_seconds=runtime*cpus_alloc
        for (edge, n_edge) in zip(core_seconds_edges[:-1],
                                  core_seconds_edges[1:]):
            if core_seconds<=n_edge:
                return edge
        return core_seconds_edges[-1]
        
    def fill_job_values(self, start=None, stop=None, append=False):
        """Calculates and stores in memory the time job values from the
        loaded trace.
        Arguments:
        - start: epoch timestamp of the submit time of the first job to be
            taken into account.
        - stop: epoch timestamp of the submit time of the last job to be taken
            into account.
        - append: if True, already stored values are not deleted and accumulated
            with new. if False previous values are deleted.
        """
        (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
         jobs_cpus_alloc, jobs_slowdown) = self._get_job_times(
                                                            submit_start=start, 
                                                            submit_stop=stop,
                                                            only_non_wf=True)
        if not append:
            self._jobs_runtime = []
            self._jobs_waittime = []
            self._jobs_turnaround = []
            self._jobs_timelimit = []
            self._jobs_cpus_alloc = []
            self._jobs_slowdown = []

        self._jobs_runtime += jobs_runtime
        self._jobs_waittime += jobs_waittime
        self._jobs_turnaround += jobs_turnaround
        self._jobs_timelimit += jobs_timelimit
        self._jobs_cpus_alloc += jobs_cpus_alloc
        self._jobs_slowdown+= jobs_slowdown

    
    def calculate_job_results_grouped_core_seconds(self,
                                                   core_seconds_edges,
                                                   store=False,
                                                   db_obj=None,
                                                   trace_id=None,
                                                   start=None,
                                                   stop=None,
                                                   append=False):
        """Calculates statistics over regular jobs on the stored trace grouped
        by the core seconds they allocate. It can
        also store the results in the database, updating the trace results entry
        with the IDs of results.
        Args:
        - core_seconds_edges: edges of the core seconds allocated by each group
            of jobs.
        - store: if True, results are stored. Also if true, the rest of
            arguments must be set.
        - db_obj: DBManager object configured to connect to a database.
        - trace_id: int with the id of analyzed trace. Entry has to exist in
            the trace results table.
        - start: integer epoch date. If set, jobs submitted before start will
            not be used in the analysis.
        - stop: integer epoch date. If set, jobs submitted after stop will
            not be used in the analysis.
        """

        data_list_of_dics=list(self.get_job_times_grouped_core_seconds(
                                            core_seconds_edges,
                                            start,
                                            stop,
                                            only_non_wf=True))
        if not append:
            self._data_list_of_dics=[{} for x in range(len(data_list_of_dics))]
        for (l1, l2) in zip(self._data_list_of_dics, data_list_of_dics):
            for key in l2.keys():
                if not key in l1.keys():
                    l1[key]=[]
                l1[key]+=l2[key]
        results = {}
        for edge in core_seconds_edges:
            data_list = [x[edge] for x in self._data_list_of_dics[:-1]]
            field_list=["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                        "jobs_requested_wc", "jobs_cpus_alloc", "jobs_slowdown"]
            field_list=["g"+str(edge)+"_"+x for x in field_list]
            bin_size_list = [60,60,120, 1, 24, 100]
            minmax_list = [(0, 3600*24*30), (0, 3600*24*30), (0, 2*3600*24*30),
                           (0, 60*24*30), (0, 24*4000), (0, 800)]
            
            results[edge]=calculate_results(data_list, field_list,
                          bin_size_list,
                          minmax_list, store=store, db_obj=db_obj, 
                          trace_id=trace_id)
        return results
    def calculate_and_store_job_results(self, store=False, db_obj=None,
                                        trace_id=None):
        data_list = [self._jobs_runtime, self._jobs_waittime, 
                     self._jobs_turnaround,
                     self._jobs_timelimit,
                     self._jobs_cpus_alloc,
                     self._jobs_slowdown]
        field_list=["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                    "jobs_requested_wc", "jobs_cpus_alloc", "jobs_slowdown"]
        bin_size_list = [60,60,120, 1, 24, 100]
        minmax_list = [(0, 3600*24*30), (0, 3600*24*30), (0, 2*3600*24*30),
                       (0, 60*24*30), (0, 24*4000), (0,800)]
        
        self.jobs_results=calculate_results(data_list, field_list,
                      bin_size_list,
                      minmax_list, store=store, db_obj=db_obj, 
                      trace_id=trace_id)
        return self.jobs_results
        
        
    def calculate_job_results(self, store=False, db_obj=None, trace_id=None,
                              start=None, stop=None):
        """Calculates statistics over regular jobs on the stored trace. It can
        also store the results in the database, updating the trace results entry
        with the IDs of results.
        Args:
        - store: if True, results are stored. Also if true, the rest of
            arguments must be set.
        - db_obj: DBManager object configured to connect to a database.
        - trace_id: int with the id of analyzed trace. Entry has to exist in
            the trace results table.
        - start: integer epoch date. If set, jobs submitted before start will
            not be used in the analysis.
        - stop: integer epoch date. If set, jobs submitted after stop will
            not be used in the analysis.
        """
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")

        self.fill_job_values(start=start, stop=stop, append=False)
        return self.calculate_and_store_job_results(store=store, db_obj=db_obj,
                                        trace_id=trace_id)
      
    def do_workflow_pre_processing(self, append=False, do_processing=True):
        """Identifies the workflows present in the loaded trace. Store the info
        internally.
        Args:
        - append: if True, already stored workflows are kept. Cleaned otherwise.
        Returns: List of identified workflows.   
        """
        if not append:
            self._wf_extractor = WorkflowsExtractor()
        self._wf_extractor.extract(self._lists_submit,
                                   reset_workflows=not append)
        if do_processing:
            self._wf_extractor.do_processing()
        return self._wf_extractor._workflows
    
    def truncate_workflows(self, num_workflows):
        self._wf_extractor.truncate_workflows(num_workflows)
    
    def rename_workflows(self, pre_number):
        self._wf_extractor.rename_workflows(pre_number)
    
    def fill_workflow_values(self, start=None, stop=None, append=False):
        """Calculate values to be analyzed for the worflows in the loaded trace.
         Arguments:
        - start: epoch timestamp of the submit time of the first job to be
            taken into account.
        - stop: epoch timestamp of the submit time of the last job to be taken
            into account.
        - append: if True, already stored values are not deleted and accumulated
            with new. if False previous values are deleted.
        """
        self._wf_extractor.fill_overall_values(start=start, stop=stop, 
                                               append=append)
        self._wf_extractor.fill_per_manifest_values(start=start, stop=stop,
                                                    append=append)
    def calculate_and_store_workflow_results(self, store=False, db_obj=None,
                                        trace_id=None):
        """Calculates statistics over workflows jobs on the stored trace. It can
        also store the results in the database, updating the trace results entry
        with the IDs of results. It runs both overal and per workflow analysis.
        Args:
        - store: if True, results are stored. Also if true, the rest of
            arguments must be set.
        - db_obj: DBManager object configured to connect to a database.
        - trace_id: int with the id of analyzed trace. Entry has to exist in
            the trace results table.
        """
        self._wf_extractor.calculate_and_store_overall_results(store=store,
                                                            db_obj=db_obj,
                                                            trace_id=trace_id)
        self._wf_extractor.calculate_per_manifest_results(store=store,
                                                            db_obj=db_obj,
                                                            trace_id=trace_id)
                
    def calculate_workflow_results(self, store=False, db_obj=None,
                                   trace_id=None, start=None, stop=None,
                                   limited=False):
        """Calculates statistics over workflows jobs on the stored trace. It can
        also store the results in the database, updating the trace results entry
        with the IDs of results.
        Args:
        - store: if True, results are stored. Also if true, the rest of
            arguments must be set.
        - db_obj: DBManager object configured to connect to a database.
        - trace_id: int with the id of analyzed trace. Entry has to exist in
            the trace results table.
        - start: integer epoch date. If set, WFs submitted before start will
            not be used in the analysis.
        - stop: integer epoch date. If set, WFs submitted after stop will
            not be used in the analysis.
        """
        
        self.workflow_results= self._wf_extractor.calculate_overall_results(
                                                    store=store,
                                                     db_obj=db_obj,
                                                    trace_id=trace_id,
                                                    start=start,
                                                    stop=stop,
                                                    limited=limited)
        
        self.workflow_results_per_manifest = (
                            self._wf_extractor.calculate_per_manifest_results(
                                                    store=store,
                                                    db_obj=db_obj,
                                                    trace_id=trace_id,
                                                    start=start,
                                                    stop=stop,
                                                    limited=limited))
    def _get_job_run_info(self, fake_stop_time=None):
        """Returns three lists of values, in each list, same position
        corresponds to the same job's information. Discards jobs with 0 in
        any of its characteristics.
        Args:
        - fake_stop_time: int epoch timestamp, if set, this value will be used
            for jobs with start time but no end time. This avoids discarding
            jobs that have end time = 0.
        Returns:
        - jobs_runtime: list of runtimes in seconds.
        - jobs_start_time: list of epoch job's start timestamps.
        - job_cores: list of number of cores allocated to each job.
        """
        jobs_runtime = []
        jobs_start_time = []
        jobs_cores = []
        for (end, start, cores) in zip(self._lists_start["time_end"], 
                                        self._lists_start["time_start"],
                                        self._lists_start["cpus_alloc"]):
            if end == 0 or start==0 or cores==0 or end<start:
                if fake_stop_time and start and not end:
                    end=fake_stop_time
                else:
                    continue
            jobs_runtime.append(end-start)
            jobs_start_time.append(start)
            jobs_cores.append(cores)
        return jobs_runtime, jobs_start_time, jobs_cores
    
    def calculate_utilization(self, max_cores, do_preload_until=None,
                              endCut=None, store=False, db_obj=None, 
                              trace_id=None,
                              ending_time=None):
        """ Produces a list of utilization changes and integrated utilization on
        the stored trace. It takes into account the utilization lost in
        single job workflows.
        Args:
        - max_cores: number of allocated cores to be considered 100%
            utilization.
        - do_preload_until: numeric epoch time stamp from which the utilization
            calculation will be performed. Jobs before that timestamp will be
            "preloaded", i.e. processed to know that load status of the machine
            when the utilization analysis starts. If set to None, the trace is
            processed from the first job.
        - endCut: numeric epoch time stamp to which the utilization calculation
            will be performed. If set to None, the traces is processed to the
            last job.
        - ending_time: If set to an int epoch timestamp, this value is used as
            end time for those jobs still running when the analysis finishes.
        Returns: 
        - integrated_ut: float between 0.0-1.0 expressing the integrated
            utilization during the analyzed period.
        - utilization_timestamps: list of epoch timestamps pointing to the
            moments of time where utilization changes happened.
        - utilization_values: list integers with the same lenght as
            utilization_timestamps. Each element corresponds to the number of
            allocated cores when an utilization change happens.    
        - acc_waste: cumulative core*seconds not used inside of single job
            workflows.
        """
        uEngine=UtilizationEngine()
        jobs_runtime, jobs_start_time, jobs_cores = self._get_job_run_info(
                                                fake_stop_time=ending_time)
        
        # Loading jobs so the "machine" is "full" by the time we start
        # measuring.
        if do_preload_until:
            uEngine.processUtilization(
                    jobs_start_time, jobs_runtime, jobs_cores,
                    doingPreload=True, 
                    endCut=do_preload_until)
        
        self._utilization_timestamps, self._utilization_values= \
                uEngine.processUtilization(
                    jobs_start_time, jobs_runtime, jobs_cores, endCut=endCut,
                    startCut=do_preload_until, 
                    preloadDone=(do_preload_until is not None))
        self._acc_waste = 0
        # Sub wasted utilization within single job workflows. 
        if self._wf_extractor:
            (stamps_list,wastedelta_list, self._acc_waste) = (
                                    self._wf_extractor.get_waste_changes())
            self._utilization_timestamps, self._utilization_values= \
                    uEngine.apply_waste_deltas(stamps_list, wastedelta_list, 
                                       start_cut=do_preload_until,
                                       end_cut=endCut)
       
        self._integrated_ut=uEngine.getIntegralUsage(maxUse=max_cores)
        self._corrected_integrated_ut=self._calculate_corrected_ut(
                                            self._integrated_ut,
                                            self._acc_waste,
                                            max_cores,
                                            self._utilization_timestamps[-1]-
                                            self._utilization_timestamps[0])
        if store:
            res = self._get_utilization_result()
            res.set_dic(dict(utilization=self._integrated_ut,
                             waste=self._acc_waste,
                             corrected_utilization =
                                 self._corrected_integrated_ut))
            res.store(db_obj, trace_id, "usage")
        return (self._integrated_ut, self._utilization_timestamps,
                self._utilization_values,
                self._acc_waste, self._corrected_integrated_ut)
    def _calculate_corrected_ut(self, integrated_ut, acc_waste, max_cores,
                               running_time_s):
        total_core_s=max_cores*running_time_s
        used_core_s=total_core_s*integrated_ut
        corrected_used_cores_s=used_core_s-acc_waste
        return float(corrected_used_cores_s)/float(total_core_s)
        
    def _get_job_wait_info(self, fake_stop_time=None, fake_start_time=None):
        jobs_runtime = []
        jobs_start_time = []
        jobs_cores = []
        jobs_submit_time = []
        for (end, start, cores, submit) in zip(self._lists_submit["time_end"], 
                                             self._lists_submit["time_start"],
                                             self._lists_submit["cpus_alloc"],
                                             self._lists_submit["time_submit"]):
            if cores==0 or start==0 or end==0 or start>end:
                if fake_start_time and not start:
                    start = fake_start_time
                    end = fake_start_time
                elif fake_stop_time and start and not end:
                    end=max(start,fake_stop_time)
                else:
                    continue
            jobs_runtime.append(end-start)
            jobs_start_time.append(start)
            jobs_cores.append(cores)
            jobs_submit_time.append(submit)
        return jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time
    
    def _get_job_wait_info_all(self):
        jobs_runtime = []
        jobs_start_time = []
        jobs_cores = []
        jobs_submit_time = []
        jobs_timelimit = []
        jobs_accuracy=[]
        ended_jobs=0.0
        accuracy=0
        
        for (end, start, cores, submit, timelimit) in zip(
                                             self._lists_submit["time_end"], 
                                             self._lists_submit["time_start"],
                                             self._lists_submit["cpus_alloc"],
                                             self._lists_submit["time_submit"],
                                             self._lists_submit["timelimit"]):
            
            if cores==0 or submit==0 or timelimit==0:
                continue
            if start==0 or end==0:
                runtime = -1
            else:
                runtime = end-start
            
            if start!=0 and end!=0:
                ended_jobs+=1
                accuracy+=float(runtime)/float(timelimit*60)
                jobs_accuracy.append(float(runtime)/float(timelimit*60))
        
            jobs_runtime.append(runtime)
            jobs_start_time.append(start)
            jobs_cores.append(cores)
            jobs_submit_time.append(submit)
            jobs_timelimit.append(timelimit)
        accuracy/=ended_jobs
        return (jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time,
                jobs_timelimit, accuracy, np.median(jobs_accuracy))
    def calculate_waiting_submitted_work_all(self, acc_period=60,
                                                     ending_time=None):
        """Calculates how much work has been submitted vs. time passed and
        how many core-s. are waiting at any given time. 
        Args:
        - acc_period: number of seconds to be used to sample the submitted
            work values (value produced is average over that time).
        - ending_time: if set, it will be used as end and start time for jobs
            which any of the two is 0 (and thus, will not be discarded)
        Returns:
        - waiting_work_stamps: ordered list of epoch timestamps of each
            datapoint of waiting_work_times.
        - waiting_work_times: each data point, how many cores seconds are
            waiting to be processed in the waiting queue.
        - submitted_work_stamps: ordered list of epoch timestamps of each
            datapoint of submitted_work_values.
        - submitted_work_values:each data point represents what share of the
            produced core hours by the system until the current time is
            required to process all the core hours submitted until the
            current time.
            
        """
        (jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time,
                jobs_timelimit, 
                mean_accuracy, median_accuracy) = self._get_job_wait_info_all()
                
        print "Observed accuracy:", mean_accuracy, median_accuracy
        accuracy = mean_accuracy
        wait_ch_events = {}
        wait_requested_ch_events = {}
        submitted_core_h_per_min = {}
        submitted_requested_core_h_per_min = {}
        
        first_time_stamp=None
        previous_stamp=None 
        acc_submitted_work=0
        acc_submitted_requested_work=0
        
        
        for submit_time, start_time, runtime, cores, timelimit in zip(
                                                           jobs_submit_time,
                                                           jobs_start_time,
                                                           jobs_runtime,
                                                           jobs_cores,
                                                           jobs_timelimit):
            
            if runtime<0:
                runtime = float(timelimit*60)*accuracy
            
            core_h = cores*runtime
            requested_core_h = timelimit*cores*60
            
            # Values for the submitted work
            if first_time_stamp is None:
                first_time_stamp=submit_time
                previous_stamp = submit_time
            acc_submitted_work+=core_h
            acc_submitted_requested_work+=requested_core_h
            if submit_time-previous_stamp>acc_period:
                corrected_acc = (float(acc_submitted_work) / 
                                 float(submit_time-first_time_stamp))
                submitted_core_h_per_min[submit_time]=corrected_acc
                corrected_requested_acc=(float(acc_submitted_requested_work) / 
                                 float(submit_time-first_time_stamp))
                submitted_requested_core_h_per_min[submit_time]= (
                                        corrected_requested_acc)
               
                previous_stamp=submit_time
            # Values for the waiting work
            if submit_time>0:
                if not submit_time in wait_ch_events.keys():
                    wait_ch_events[submit_time]=0
                    wait_requested_ch_events[submit_time]=0
                wait_ch_events[submit_time]+=core_h
                wait_requested_ch_events[submit_time]+=requested_core_h
            if start_time>0:
                if not start_time in wait_ch_events.keys():
                    wait_ch_events[start_time]=0
                    wait_requested_ch_events[start_time]=0
                wait_ch_events[start_time]-=core_h
                wait_requested_ch_events[start_time]-=requested_core_h
        
        # Final step to calculate the waiting work
        
        acc_ch = 0
        acc_requested_ch = 0
       
        stamps  = []
        waiting_ch = []
        waiting_requested_ch=[]
       
        for stamp in sorted(wait_ch_events.keys()):
            acc_ch+=wait_ch_events[stamp]
            acc_requested_ch+=wait_requested_ch_events[stamp]
            stamps.append(stamp)
            waiting_ch.append(acc_ch)
            waiting_requested_ch.append(acc_requested_ch)
        
        # Making sure stamps are orderd for the produced work.
        core_h_per_min_stamps=[]
        core_h_per_min_values=[]
        requested_core_h_per_min_values=[]
        previous_stamp=None 
        for stamp in sorted(submitted_core_h_per_min.keys()):
            core_h_per_min_stamps.append(stamp)
            core_h_per_min_values.append(submitted_core_h_per_min[stamp])
            requested_core_h_per_min_values.append(
                                   submitted_requested_core_h_per_min[stamp])
        return (stamps, waiting_ch, core_h_per_min_stamps, core_h_per_min_values,
            waiting_requested_ch, requested_core_h_per_min_values)
    
    def calculate_waiting_submitted_work(self, acc_period=60,
                                         ending_time=None):
        """Calculates how much work has been submitted vs. time passed and
        how many core-s. are waiting at any given time. 
        Args:
        - acc_period: number of seconds to be used to sample the submitted
            work values (value produced is average over that time).
        - ending_time: if set, it will be used as end and start time for jobs
            which any of the two is 0 (and thus, will not be discarded)
        Returns:
        - waiting_work_stamps: ordered list of epoch timestamps of each
            datapoint of waiting_work_times.
        - waiting_work_times: each data point, how many cores seconds are
            waiting to be processed in the waiting queue.
        - submitted_work_stamps: ordered list of epoch timestamps of each
            datapoint of submitted_work_values.
        - submitted_work_values:each data point represents what share of the
            produced core hours by the system until the current time is
            required to process all the core hours submitted until the
            current time.
            
        """
        jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time = (
                        self._get_job_wait_info(fake_stop_time=ending_time,
                                                fake_start_time=ending_time))
        wait_ch_events = {}
        submitted_core_h_per_min = {}
        
        first_time_stamp=None
        previous_stamp=None 
        acc_submitted_work=0
        
        
        for submit_time, start_time, runtime, cores in zip(jobs_submit_time,
                                                           jobs_start_time,
                                                           jobs_runtime,
                                                           jobs_cores):
            if runtime<0:
                raise Exception()
            core_h = cores*runtime
            # Values for the submitted work
            if first_time_stamp is None:
                first_time_stamp=submit_time
                previous_stamp = submit_time
            acc_submitted_work+=core_h
            if submit_time-previous_stamp>acc_period:
                corrected_acc = (float(acc_submitted_work) / 
                                 float(submit_time-first_time_stamp))
                submitted_core_h_per_min[submit_time]=corrected_acc
                #acc_submitted_work=0
                previous_stamp=submit_time
            # Values for the waiting work
            if submit_time!=0:
                if not submit_time in wait_ch_events.keys():
                    wait_ch_events[submit_time]=0
                wait_ch_events[submit_time]+=core_h
            if start_time!=0:
                if not start_time in wait_ch_events.keys():
                    wait_ch_events[start_time]=0
                wait_ch_events[start_time]-=core_h
        
        # Final step to calculate the waiting work
        acc_ch = 0
       
        stamps  = []
        waiting_ch = []
       
        for stamp in sorted(wait_ch_events.keys()):
            acc_ch+=wait_ch_events[stamp]
            stamps.append(stamp)
            waiting_ch.append(acc_ch)
        
        # Making sure stamps are orderd for the produced work.
        core_h_per_min_stamps=[]
        core_h_per_min_values=[]
        previous_stamp=None 
        for stamp in sorted(submitted_core_h_per_min.keys()):
            core_h_per_min_stamps.append(stamp)
            core_h_per_min_values.append(submitted_core_h_per_min[stamp])
        return stamps, waiting_ch, core_h_per_min_stamps, core_h_per_min_values
        
    def _get_utilization_result(self):
        return NumericList("usage_values", ["utilization", "waste",
                                            "corrected_utilization"])
    
    def get_utilization_values(self):
        """Returns: float 0-1 (overal integrated utilization), and integer with
        the number of core hours wasted by wingle job workflows.
        """
        return (self._integrated_ut, self._acc_waste,
                self._corrected_integrated_ut)
    
    def load_utilization_results(self, db_obj, trace_id):
        """ Creates a Utilization result object and fills the corresponding
        information.
        Args:
        - db_obj: DBManager Object used to pull the information from a database.
        - trace_id: numeric id of the trace to which the data corresponds.
        """
        res = self._get_utilization_result()
        res.load(db_obj, trace_id, "usage")
        self._acc_waste = res.get_data()["waste"]
        self._integrated_ut = res.get_data()["utilization"]
        self._corrected_integrated_ut = res.get_data()["corrected_utilization"]
    
    def calculate_utilization_median_result(self, trace_id_list, store, db_obj,
                                            trace_id):
        """Calculates and stores the medial utilzation and waste values across
        a list of traces.
        """
        integrated_values = []
        wasted_values = []
        corrected_integrated_values=[]
        for sub_trace_id in trace_id_list:
            rt = ResultTrace()
            rt.load_utilization_results(db_obj, sub_trace_id)
            integrated_values.append(rt._integrated_ut)
            wasted_values.append(rt._acc_waste)
            corrected_integrated_values.append(rt._corrected_integrated_ut)
        
        self._acc_waste = np.median(wasted_values)
        self._integrated_ut = np.median(integrated_values)
        self._corrected_integrated_ut = np.median(corrected_integrated_values)
        if store:
            res = self._get_utilization_result()
            res.set_dic(dict(utilization=self._integrated_ut,
                             waste=self._acc_waste,
                             corrected_utilization=self._corrected_integrated_ut
                             ))
            res.store(db_obj, trace_id, "usage")
   
    def calculate_utilization_mean_result(self, trace_id_list, store, db_obj,
                                            trace_id):
        """Calculates and stores the medial utilzation and waste values across
        a list of traces.
        """
        integrated_values = []
        wasted_values = []
        corrected_integrated_values=[]
        for sub_trace_id in trace_id_list:
            rt = ResultTrace()
            rt.load_utilization_results(db_obj, sub_trace_id)
            integrated_values.append(rt._integrated_ut)
            wasted_values.append(rt._acc_waste)
            corrected_integrated_values.append(rt._corrected_integrated_ut)
        self._acc_waste = np.sum(wasted_values)
        self._integrated_ut = np.mean(integrated_values)
        self._corrected_integrated_ut = np.mean(corrected_integrated_values)
        if store:
            res = self._get_utilization_result()
            res.set_dic(dict(utilization=self._integrated_ut,
                             waste=self._acc_waste,
                             corrected_utilization=self._corrected_integrated_ut
                             ))
            res.store(db_obj, trace_id, "usage_mean")
        
    def create_trace_table(self, db_obj, table_name):
        """ For testing """
        query = """
           CREATE TABLE `{0}` (
           `trace_id` INT(10) NOT NULL,
          `job_db_inx` int(11) NOT NULL,
          `account` tinytext,
          `cpus_req` int(10) unsigned NOT NULL,
          `cpus_alloc` int(10) unsigned NOT NULL,
          `job_name` tinytext NOT NULL,
          `id_job` int(10) unsigned NOT NULL,
          `id_qos` int(10) unsigned NOT NULL DEFAULT '0',
          `id_resv` int(10) unsigned NOT NULL,
          `id_user` int(10) unsigned NOT NULL,
          `nodes_alloc` int(10) unsigned NOT NULL,
          `partition` tinytext NOT NULL,
          `priority` int(10) unsigned NOT NULL,
          `state` smallint(5) unsigned NOT NULL,
          `timelimit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_submit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_start` int(10) unsigned NOT NULL DEFAULT '0',
          `time_end` int(10) unsigned NOT NULL DEFAULT '0',
          PRIMARY KEY (`trace_id`, `id_job`),
          UNIQUE KEY `main_key` (`trace_id`,`id_job`))
         """.format(table_name)
        db_obj.doUpdate(query)
    
    def create_import_table(self, db_obj, table_name):
        """ For testing """
        query = """
           CREATE TABLE `{0}` (
          `job_db_inx` int(11) NOT NULL,
          `account` tinytext,
          `cpus_req` int(10) unsigned NOT NULL,
          `cpus_alloc` int(10) unsigned NOT NULL,
          `job_name` tinytext NOT NULL,
          `id_job` int(10) unsigned NOT NULL,
          `id_qos` int(10) unsigned NOT NULL DEFAULT '0',
          `id_resv` int(10) unsigned NOT NULL,
          `id_user` int(10) unsigned NOT NULL,
          `nodes_alloc` int(10) unsigned NOT NULL,
          `partition` tinytext NOT NULL,
          `priority` int(10) unsigned NOT NULL,
          `state` smallint(5) unsigned NOT NULL,
          `timelimit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_submit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_start` int(10) unsigned NOT NULL DEFAULT '0',
          `time_end` int(10) unsigned NOT NULL DEFAULT '0',
          PRIMARY KEY (`job_db_inx`),
          UNIQUE KEY `job_db_inx` (`job_db_inx`))
         """.format(table_name)
        db_obj.doUpdate(query)


        

def _get_limit(order_field, start=None, end=None):
    """ Produces a SQL condition the order_filed to be between start and end."""
    query = ""    
    if start is None and end is None:
        return None
    if start:
        query+="{0}>={1}".format(order_field, start) 
    if end:
        if start:
            query +=" AND "
        query+="{0}<={1}".format(order_field, end)
    return query
