from stats import calculate_results, load_results, Histogram, NumericStats
from generate.pattern import WorkflowGeneratorMultijobs
import bisect
import os

class WorkflowsExtractor(object):
    """ Extracts the workflows inside of a ResultTrace object. It 
    also produces the metrics of those workflows.
    """
    def __init__(self):
        """Constructor"""
        self._workflows={}

    def extract(self, job_list, reset_workflows=False):
        """Extracts the workflows from dictionary of lists containing the
        properties of a trace jobs.
        Args:
        - job_list: A dictionary of lists. Items at the same position in all the
            lists correspond to data on the same job. It expect the dictionary
            to include at least lists indexed by: "job_name", "time_start", 
            "time_end", and "id_job".
        """
        if reset_workflows:
            self._workflows={}
        size=None
        for value in list(job_list.values()):
            if size is not None and size!=len(value):
                raise ValueError("All lists in job_list should have the same"
                                 " length.")
        
        #self._workflows = {}
        count = len(list(job_list.values())[0])
        for i in range(count):
            self.check_job(job_list, i)
        
    
    def do_processing(self):
        """ Parsed workflows depenencies are filled and critical paths explored
        """
        for wf in list(self._workflows.values()):
            wf.fill_deps()
        
    
    def check_job(self, job_list, pos):
        """ Checks if a job belongs to a workflow and puts it inside of its
        corresponding WorkflowTracker. The name of a job belonging to a workflow
        starts with "wf" and the format is wf_[manifest]-[job_id]_[stage]_[deps] 
        Args:
        - job_list: Dictionary of lists containing jobs' information. See
            extract for format.
        - pos: Position in the lists corresponding to the job to be studied.
        Returns: True if the job belongs to a workflow, False otherwise.
        - 
        """
        if job_list["time_end"][pos]==0 or job_list["time_start"][pos]==0:
            return False
        job_name = job_list["job_name"][pos]
        id_job = job_list["id_job"][pos]
        if "wf" == job_name[:2]:
            name, stage_id, deps = TaskTracker.extract_wf_name(job_name)
            if not name in list(self._workflows.keys()):
                self._workflows[name] = WorkflowTracker(name)
            self._workflows[name].register_task(job_list, pos, stage_id=="")
            return True
        else:
            return False 
    def get_workflow(self, wf_name):
        """Returns a workflows indexed by its manifest name and
        parent job_id.
        Args:
        - wf_name: a string of the format "[manfiest name]_[job id]".
        Returns: WorkflowTracker object 
        """
        return self._workflows[wf_name]
    
    def _get_workflow_times(self, submit_start=None, submit_stop=None): 
        """Extracts runtime, wait time, turn around time, stretch factor (
        interjob wait time/runtime in critical path), workflow jobs runtime,
        and workflow jobs cores from the workflows detected.
        Args:
        - submit_start: integer epoch date. If set, data of WFs submitted
            before this is not returned. 
        - submit_stop: integer epoch date. If set, data of WFs submitted
            after this is not returned.
        
        Returns a list for each variable.
        """
        wf_runtime = []
        wf_waittime = []
        wf_turnaround= []
        wf_stretch_factor = []
        wf_jobs_runtime = []
        wf_jobs_cores = []
        
        for wf in list(self._workflows.values()):
            if wf._incomplete_workflow:
                continue
            submit_time = wf.get_submittime()
            if (submit_start is not None and submit_time < submit_start):
                continue
            if (submit_stop is not None and submit_stop<submit_time):
                continue
            wf_runtime.append(wf.get_runtime())
            wf_waittime.append(wf.get_waittime())
            wf_turnaround.append(wf.get_turnaround())
            wf_stretch_factor.append(wf.get_stretch_factor())
            wf_jobs_runtime = wf_jobs_runtime + wf.get_jobs_runtime()
            wf_jobs_cores = wf_jobs_cores + wf.get_jobs_cores()
           
        return (wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores)
    
    def _get_per_manifest_workflow_times(self,
                                         submit_start=None,
                                         submit_stop=None):
        """Extracts runtime, wait time, turn around time, stretch factor (
        interjob wait time/runtime in critical path), workflow jobs runtime,
        and workflow jobs cores from the workflows detected, HOWEVER, 
        grouped per type of workflow (manifest)"
        Args:
        - submit_start: integer epoch date. If set, data of WFs submitted
            before this is not returned. 
        - submit_stop: integer epoch date. If set, data of WFs submitted
            after this is not returned.
        
        Returns a dictionary of lists for each variable indexed by manifest
        name.
        """
        manifests = {}
        for (name, wf) in self._workflows.items():
            if wf._incomplete_workflow:
                continue
            submit_time = wf.get_submittime()
            if (submit_start is not None and submit_time < submit_start):
                continue
            if (submit_stop is not None and submit_stop<submit_time):
                continue
            manifest = name.split("-")[0]
            if not manifest in list(manifests.keys()):
                manifests[manifest]=dict(wf_runtime = [],
                                        wf_waittime = [],
                                        wf_turnaround= [],
                                        wf_stretch_factor = [],
                                        wf_jobs_runtime = [],
                                        wf_jobs_cores = [])
            manifests[manifest]["wf_runtime"].append(wf.get_runtime())
            manifests[manifest]["wf_waittime"].append(wf.get_waittime())
            manifests[manifest]["wf_turnaround"].append(wf.get_turnaround())
            manifests[manifest]["wf_stretch_factor"].append(
                                                    wf.get_stretch_factor())
            manifests[manifest]["wf_jobs_runtime"] = (
                manifests[manifest]["wf_jobs_runtime"] + wf.get_jobs_runtime())
            manifests[manifest]["wf_jobs_cores"] = (
                manifests[manifest]["wf_jobs_cores"] + wf.get_jobs_cores())
        return manifests
    
    def calculate_wf_results(self, db_obj,trace_id, wf_runtime, wf_waittime,
                         wf_turnaround, wf_stretch_factor,
                         wf_jobs_runtime, wf_jobs_cores, store=False, 
                         prefix=None):
        """Calculates Histogram and NumericStats over various data lists and
        returns them in a dictionary indexed by 
        "[[prefix]_]wf_[variable type]_cdf" and 
        "[[prefix]_]wf_[variable type]_stats".
        Args:
        - db_obj: Database over to write the results if store is True.
        - trace_id: Numeric id of the trace that this data corresponds to.
        - wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor, 
            wf_jobs_runtime, wf_jobs_cores: Data to be analyzed, there will be
            two Result objects per each data set.
        - store: If set to True, Results objects will store their information
            in a database.
        - prefix: string to be added before the object name and result time
        Returns a dictionary indexed by field_list with the results
        """
        data_list = [wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores]
        field_list = ["wf_runtime", "wf_waittime", "wf_turnaround",
                      "wf_stretch_factor", "wf_jobs_runtime", "wf_jobs_cores"]
        if prefix!=None:
            field_list = [prefix+"_"+x for x in field_list]
        bin_size_list = [60,60,120, 0.01, 60, 24]
        minmax_list = [(0, 3600*24*30), (0, 3600*24*30), (0, 2*3600*24*30),
                       (0, 1000), (0, 3600*24*30), (0, 24*4000)]
        return calculate_results(data_list, field_list, bin_size_list,
                      minmax_list, store=store, db_obj=db_obj, 
                      trace_id=trace_id)            
    
    def fill_overall_values(self, start=None, stop=None, append=False):
        (wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores) = self._get_workflow_times(
                                         submit_start=start, submit_stop=stop)
        if not append:
            self._wf_runtime = []
            self._wf_waittime = []
            self._wf_turnaround = []
            self._wf_stretch_factor = []
            self._wf_jobs_runtime = []
            self._wf_jobs_cores = []
            
        self._wf_runtime += wf_runtime
        self._wf_waittime += wf_waittime
        self._wf_turnaround += wf_turnaround
        self._wf_stretch_factor += wf_stretch_factor
        self._wf_jobs_runtime += wf_jobs_runtime
        self._wf_jobs_cores += wf_jobs_cores

    def get_first_workflows(self, keys, num_workflows):
        """Returns a list of  keys of the firs num_workflows workflows."""
        return sorted(keys, key=lambda x: int(x.split("-")[-1]))[:num_workflows]
        
        
    def truncate_workflows(self, num_workflows):
        """Eliminates the detected workflows, keeping only the first
        num_workflows ones."""
        new_dic={}
        keys_sub_set = self.get_first_workflows(list(self._workflows.keys()),
                                                 num_workflows)
        for key in keys_sub_set:
            new_dic[key]  = self._workflows[key]
        self._workflows = new_dic
    def rename_workflows(self, pre_id):
        new_dic={}
        for key in list(self._workflows.keys()):
            if key[0]==".":
                if pre_id is None:
                    new_key=key[1:]
                else:
                    new_key=key
            else:
                if pre_id is not None:
                    workflow_type=key.split("-")[0]
                    workflow_id=key.split("-")[1]
                    new_key=".{0}-{1}{2}".format(workflow_type, pre_id,
                                                 workflow_id)
                else:
                    new_key=key
            new_dic[new_key]  = self._workflows[key]
        self._workflows = new_dic
    
    def calculate_and_store_overall_results(self, store=False, db_obj=None,
                                        trace_id=None,
                                        limited=False):
        prefix=None
        if limited:
            prefix="lim"
        return self.calculate_wf_results(db_obj,trace_id, self._wf_runtime,
                                  self._wf_waittime, self._wf_turnaround,
                                  self._wf_stretch_factor,
                                  self._wf_jobs_runtime,
                                  self._wf_jobs_cores, store=store,
                                  prefix=prefix)

    def calculate_overall_results(self, store=False, db_obj=None,trace_id=None,
                                  start=None, stop=None, limited=False):
        """Produce analysis on the variables of the workflows and returns
        them in a dictionary indexed by 
        "wf_[variable type]_cdf" and "wf_[variable type]_stats".
        Args:
        - Store: if True, results are stored in a database.
        - db_obj: DBManager object used to write on a database.
        - trace_id: numeric id of the trace associated with the analyzed data
        - start: integer epoch date. If set, WFs submitted before start will
            not be used in the analysis.
        - stop: integer epoch date. If set, WFs submitted after stop will
            not be used in the analysis.
        """
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")
        
        self.fill_overall_values(start=start, stop=stop, append=False)
        return self.calculate_and_store_overall_results(store=store,
                                                        db_obj=db_obj,
                                                        trace_id=trace_id,
                                                        limited=limited)
    
    def load_wf_results(self, db_obj, trace_id, prefix=None):
        """Retrieves analysis on workflow variables from the trace trace_id
        and returns them in a dictionary indexed by 
        "[[prefix]_]wf_[variable type]_cdf" and
        "[[prefix]_]wf_[variable type]_stats".
        Args:
        - db_obj: DBManager object used to write on a database.
        - trace_id: numeric id of the trace associated with the analyzed data
        - prefix: string to be prepended to the result types.
        """
        field_list = ["wf_runtime", "wf_waittime", "wf_turnaround",
                      "wf_stretch_factor", "wf_jobs_runtime", "wf_jobs_cores"]
        if prefix!=None:
            field_list = [prefix+"_"+x for x in field_list]
        return load_results(field_list, db_obj, trace_id)
        
    def load_overall_results(self, db_obj, trace_id):
        """Retrieves analysis on overall workflow variables from the trace
        trace_id and returns them in a dictionary indexed by 
        "wf_[variable type]_cdf" and "wf_[variable type]_stats".
        Args:
        - db_obj: DBManager object used to write on a database.
        - trace_id: numeric id of the trace associated with the analyzed data
        """
        return self.load_wf_results(db_obj, trace_id)

    def fill_per_manifest_values(self, start=None, stop=None, append=False):
        if not append:
            self._manifests_values = {}
        
        new_manifests_values = self._get_per_manifest_workflow_times(
                                                        submit_start=start,
                                                        submit_stop=stop)
        
        for man_name in new_manifests_values:
            if man_name in list(self._manifests_values.keys()):
                self._manifests_values[man_name] = (
                    WorkflowsExtractor.join_dics_of_lists(
                                        self._manifests_values[man_name],
                                        new_manifests_values[man_name]))
            else:
                self._manifests_values[man_name]=new_manifests_values[man_name]  
        self._detected_manifests=list(self._manifests_values.keys())
        
    
    def calculate_and_store_per_manifest_results(self, store=False, db_obj=None,
                                        trace_id=None,limited=False):
        prefix=""
        if limited:
            prefix="lim_"
        results_per_manifest = {}
        for (manifest, data) in self._manifests_values.items():
            results_per_manifest[manifest]= self.calculate_wf_results(
                db_obj,trace_id,
                data["wf_runtime"], data["wf_waittime"],
                data["wf_turnaround"], data["wf_stretch_factor"],
                data["wf_jobs_runtime"], data["wf_jobs_cores"],
                store=store,prefix=prefix+"m_"+manifest)
        return results_per_manifest
        
    def calculate_per_manifest_results(self, store=False,db_obj=None, 
                                       trace_id=None, start=None, stop=None,
                                       limited=False):
        """Produce analysis on the variables of the each type of workflow
        detected (by manifest) and  returns them as a dictionary of dictionaries
        of Result objects. First level indexed by the manifest type and the
        second "wf_[variable type]_cdf" and "wf_[variable type]_stats".
        Args:
        - Store: if True, results are stored in a database.
        - db_obj: DBManager object used to write on a database.
        - trace_id: numeric id of the trace associated with the analyzed data.
        - start: integer epoch date. If set, WFs submitted before start will
            not be used in the analysis.
        - stop: integer epoch date. If set, WFs submitted after stop will
            not be used in the analysis.
        - limited: True if results belong to second pass. Results will be
            stored with a "lim_" prefix.
        """
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")
        self.fill_per_manifest_values(start=start, stop=stop, append=False)
        return self.calculate_and_store_per_manifest_results(store=store,
                                                            db_obj=db_obj,
                                                            trace_id=trace_id,
                                                            limited=limited)
    def load_per_manifest_results(self, db_obj, trace_id):
        """Retrieves analysis on per workflow manigest variables from the trace
        trace_id and returns them as a dictionary of dictionaries
        of Result objects. First level indexed by the manifest type and the
        second "wf_[variable type]_cdf" and "wf_[variable type]_stats".
        Args:
        - db_obj: DBManager object used to write on a database.
        - trace_id: numeric id of the trace associated with the analyzed data
        """  
        per_manifest_results = {}      
        self._detected_manifests = self._get_manifests_in_db(db_obj, trace_id)
        for manifest in self._detected_manifests:
            per_manifest_results[manifest]=self.load_wf_results(db_obj, 
                                                trace_id, prefix="m_"+manifest)
        return per_manifest_results
            
    def _get_manifests_in_db(self, db_obj, trace_id):
        """Returns a list of present workflow types in a trace in the DB.
        """
        hist = Histogram()
        hist_man = _filter_non_man(hist.get_list_of_results(db_obj, trace_id))
        stats = NumericStats()
        stats_man =  _filter_non_man(stats.get_list_of_results(db_obj, trace_id))
        
        hist_man=[x.split("_")[1] for x in hist_man]
        stats_man=[x.split("_")[1] for x in stats_man]
        return  list(set(hist_man+stats_man))
    def get_waste_changes(self):
        """
        Produces a time-stamped list of deltas of the number of cores wasted
        by single job workflows.
        Returns:
        - stamps_list: epoch time stamps of the waste deltas
        - wastedelta_list: List of changes in current waste. A positive value
            implies that more resources are wasted, a negative value, less
            resources are wasted. This list must sum up 0.
        - acc_waste: number if core-seconds wasted in the resulting time
            period.
        """
        stamps_list = []
        wastedelta_list =[]
        acc_waste=0 
        for wf in list(self._workflows.values()):
            stamps, usage, acc = wf.get_waste_changes()
            acc_waste+=acc
            stamps_list, wastedelta_list = _fuse_delta_lists(stamps_list, 
                                                          wastedelta_list,
                                                          stamps, usage)
        return stamps_list, wastedelta_list, acc_waste
    
    @classmethod   
    def join_dics_of_lists(self, dic1, dic2):
        """ Returns a new dictionary: joins two dictionaries of lists """
        new_dic = {}
        keys = list(dic1.keys())+list(dic2.keys())
        keys = list(set(keys))
        for key in keys:
            new_dic[key]=[]
            if key in list(dic1.keys()):
                new_dic[key]+=dic1[key]
            if key in list(dic2.keys()):
                new_dic[key]+=dic2[key]
        return new_dic

def _fuse_delta_lists(stamps_list, deltas_list, stamps, deltas):
    """Joins two lists of waste deltas"""
    for (st, us) in zip(stamps, deltas):
        pos = bisect.bisect_left(stamps_list, st)
        if (pos<len(stamps_list)) and stamps_list[pos]==st:
            deltas_list[pos]+=us
        else:
            stamps_list.insert(pos, st)
            deltas_list.insert(pos, us)
    return stamps_list, deltas_list       
            
    
def _filter_non_man(manifests):
    return [ x for x in manifests if x[0:2]=="m_"]
    

class WorkflowTracker(object):
    """ Object to store the information of a trace workflow, its subtasks and
    charateristics.
    """
    def __init__(self, name):
        """Constructor
        Args:
        - name: string indenritying the workflow in the trace. Generally its
            "[manifest_name]_[parent job id]".
        """
        self._name = name
        self._tasks={}
        self._critical_path=[]
        self._critical_path_runtime=0
        self._parent_job=None
        self._incomplete_workflow=False;
    
    def get_runtime(self):
        return self._critical_path_runtime
    
    def get_waittime(self):
        if self._parent_job is not None:
            return self._parent_job.get_waittime()
        return (self.get_first_task().get_waittime())        
    def get_turnaround(self):
        return self.get_runtime()+self.get_waittime()
    
    def get_submittime(self):
        return  (self.get_first_task().get_submittime())  
    
    def get_stretch_factor(self):
        acc_wait = self.get_waittime()
        for (t1,t2) in zip(self._critical_path[:-1], self._critical_path[1:]):
            acc_wait += t2.data["time_start"]-t1.data["time_end"]
        return float(acc_wait)/float(self.get_turnaround())
    
    def get_jobs_runtime(self):
        return [x.get_runtime() for x in self.get_all_tasks()]
    
    def get_jobs_cores(self):
        return [x.get_cores() for x in self.get_all_tasks()]
    
    def register_task(self, job_list, pos, parent_job=False):
        """Adds the job in position pos in job_list as a task to this workflow.
        """
        task = TaskTracker(job_list, pos, self)
        if parent_job:
            self._parent_job=task
        else:
            self._tasks[task.stage_id] = task
    
    def get_first_task(self):
        if len(self._tasks)==0:
            return self._parent_job
        else:
            return self._critical_path[0]
    def get_all_tasks(self):
        if len(self._tasks)==0:
            return [self._parent_job]
        else:
            return list(self._tasks.values())
            

    
    def fill_deps(self):
        """Resolves the dependencies and critical path by parsing the
        dependecies included in the job name.
        """
        if len(self._tasks)==0 and self._parent_job is None:
            raise ValueError("Workflow has no tasks inside")
        elif len(self._tasks)==0:
            self.single_job_wf=True
        else:
            self.single_job_wf=False
        start_task=None
        for task in self.get_all_tasks():
            if not task.deps:
                if not start_task:
                    start_task = task
                elif start_task.data["time_start"]>task.data["time_start"]:
                    start_task = task
            else:
                for dep in task.deps:
                    if dep in list(self._tasks.keys()):
                        self._tasks[dep].add_dep_to(task)
                    else:
                        self._incomplete_workflow=True
        self._start_task=start_task
        self._critical_path, self._critical_path_runtime = (
                                            self._get_critical_path(start_task))
        self._incomplete_workflow = not self._critical_path 
        
    def _get_critical_path(self, task, min_time=0):
        """Returns critical path from task to the end of the workflow if its
        lenght (including tasks runtime and wait timeI is longer thatn min_time.
        """
        path = []
        if self._incomplete_workflow:
            return [], 0
        task_runtime=task.data["time_end"]-task.data["time_start"]
        
        path_time = 0
        for sub_task in task.dependenciesTo:
            sub_task_wait_time=sub_task.data["time_start"]-task.data["time_end"]     
            sub_path, sub_time = self._get_critical_path(sub_task,
                                                       path_time)
            if (sub_path and task_runtime+sub_task_wait_time+sub_time>min_time
                and task_runtime+sub_task_wait_time+sub_time>path_time):
                path=sub_path
                path_time=sub_task_wait_time+sub_time
        
        path = [task] + path
        path_time += task_runtime
        if min_time<path_time:
            return path, path_time
        
        return [], 0
    
    def get_waste_changes(self):
        if not self.single_job_wf:
            return [], [], 0
        else:
            manifest = "-".join(self._name.split("-")[0:-1])
            we = WasteExtractor(manifest)
            return we.get_waste_changes(self._start_task.data["time_start"])
            
def paint_path(path):  
    return [t.stage_id for t in path]

class WasteExtractor(object):
    """Class to extract the wasted core hours by single job workflows in
    comparison to multijob and manifest driven workflows.
    """
    def __init__(self, manifest):
        self._manifest = manifest
        from orchestration.running import ExperimentRunner
        man_dir = os.getenv("MANIFEST_DIR",
                            ExperimentRunner.get_manifest_folder())
        self._manifest = os.path.join(man_dir, self._manifest)
    
    def get_waste_changes(self, start_time):
        self._time_stamps = []
        self._allocation_changes = []
        total_cores, total_runtime=self._expand_workflow(self._manifest,
                                                     start_time)
        
        waste_list = []
        waste = None
       
        for x in self._allocation_changes:
            if waste==None:
                waste = total_cores - x
                waste_list.append(waste)
            else:
                new_waste = waste - x
                waste_list.append(new_waste-waste)
        waste_list[-1]-=total_cores
        return self._time_stamps, waste_list, self.get_acc_waste(
                                                            self._time_stamps,
                                                            waste_list)

    def get_acc_waste(self, stamp_list, waste_list):
        current_waste=0
        acc_waste=0
        for (s1,s2,w) in zip(stamp_list[:-1], stamp_list[1:], waste_list):
            current_waste+=w
            acc_waste+=(s2-s1)*current_waste
        return acc_waste
    def _expand_workflow(self, manifest, start_time):
        total_cores, runtime, tasks = WorkflowGeneratorMultijobs.parse_all_jobs(
                                                                    manifest)
        job_count = 0
        remaining_tasks = list(tasks.values())
        while (remaining_tasks):
            new_remaining_tasks  = []
            for task in remaining_tasks:
                if self._task_can_run(task):
                    job_count+=1
                    feasible_start_time=self._get_feasible_start_time(task,
                                                                    start_time)
                    task["time_end"]=feasible_start_time+task["runtime_sim"]
                    cores = task["number_of_cores"]
                    self._add_job_change(feasible_start_time, cores)
                    self._add_job_change(task["time_end"], -cores)
                    task["job_id"]=job_count
                else:
                    new_remaining_tasks.append(task)
            remaining_tasks=new_remaining_tasks
        return total_cores, runtime
    
    def _add_job_change(self, time_stamp, cores):
        pos=bisect.bisect_left(self._time_stamps, time_stamp)
        if pos<len(self._time_stamps) and self._time_stamps[pos]==time_stamp: 
            self._allocation_changes[pos]+= cores 
        else:
            self._time_stamps.insert(pos, time_stamp)
            self._allocation_changes.insert(pos, cores)
        
            
            

    def _task_can_run(self, task):
        if len(task["dependencyFrom"])==0:
            return True
        for task_dep in task["dependencyFrom"]:
            if not "job_id" in list(task_dep.keys()):
                return False
        return True
    def _get_feasible_start_time(self, task, start_time):
        if task["dependencyFrom"] == []:
            return start_time
        else:
            return max([x["time_end"] for x in task["dependencyFrom"] ])
        
                
    
    
        
        
class TaskTracker(object):
    """ Stores information of a job within a workflow """
    def __init__(self, job_list, pos, parent_workflow):
        """ Constructor. It will retrieve the information in position pos and
        infer its role in the workflow by parsing its job_name string. 
        Args:
        - job_list: jobs in the trace.
        - pos: position of the job to parse.
        - parent_workflow: workflow to which this task belongs. 
        """
        self.data = {}
        for key in list(job_list.keys()):
            self.data[key]=job_list[key][pos]
        
        self.name=self.data["job_name"]
        self.job_id=self.data["id_job"]
        self._parent_workflow=parent_workflow
        self.dependenciesTo = []        
        self.wf_name, self.stage_id, self.deps = (
                                    TaskTracker.extract_wf_name(self.name))
        
    def add_dep_to(self, task):
        """Adds a TaskTracker object that depends on this TastTracker object"""
        self.dependenciesTo.append(task)
    def get_runtime(self):
        return self.data["time_end"]-self.data["time_start"]
    def get_waittime(self):
        return self.data["time_start"]-self.data["time_submit"]
    def get_cores(self):
        return self.data["cpus_alloc"]
    def get_submittime(self):
        return self.data["time_submit"]
    @classmethod    
    def extract_wf_name(self, wf_name):
        """ Extract information on the role of a TaskTracker from its job name.
        Args:
        - wf_name, string: Expected format is
            wf_[manifiest]-[job_id]_S[stage#]_[d1-d2-d3]
            - manifest: manifets file describing the worfklow
            - job_id: job id of the parent job containing the workflow.
            - Stage: S[n], being n a number
            - [dS1-dS2-dS3] list of "d[Sn]", where Sn is a name of stage. It
            represents the stages that this task depends on.
        Returns:
            name: string that indentifies the workflow: manifest-[idjob].
            stage_id: string with the number of stage.
            deps: list of strings with the numbers of stages the task depends
            on.
        """
        parts = wf_name.split("_")
        name = parts[1]
        stage_id=""
        deps=[]
        if len(parts)==3 and "-" in parts[2]:
            cad = parts[2]
            pos = cad.find("-")
            parts[2]=cad[:pos]
            parts.append(cad[pos+1:])
            
        if len(parts)>2:
            stage_id = parts[2][0:]
        if len(parts)>3:
            deps = [x[1:] for x in parts[3].split("-")] 
        return name, stage_id, deps