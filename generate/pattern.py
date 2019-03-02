from generate import RandomSelector, TimeController
from os import path

import datetime
import json
import pygraphviz as pgv
import os


class PatternGenerator(object):
    """ Base class to define the actual submission actions for a worklad
    behavior pattern. It will be redefined for specific cases: e.g. a
    WorkflowGenerator class that nows how to submit workflows in an
    specific format.
    """
    def __init__(self, workload_generator=None):
        """Constructor
        Args: 
            - workload_generator: WorkloadGenerator object in which this
                PatternGenereator is registered.           
        
        """
        self._workload_generator = workload_generator
   
    def do_trigger(self, create_time):
        """This method is called by the workload generator when it is time for
        this submission pattern to happen. It must use add_job to submit new
        jobs and return the number of jobs that it has submitted."""
        self._workload_generator._generate_new_job(create_time)
        return 1

class PatternTimer(object):
    """ Base class to control to trigger the action of PatternGenerator when
    the simulation time stamp is equal to a certain value. Concrete
    behaviors can be defined by extending this class and re-defining
    can_be_purged, and is_it_time. This class never triggers.
    """
    
    def __init__(self, pattern_generator, register_timestamp=None,
                 register_datetime=None):
        """Constructor. Creates object and sets the simulation time when it was
        created. Both register_timestamp and register_date_time cannot be set
        at the same time. 
        Args
            - pattern_generator: PatternGenerator object to be triggered when
                the simulation time is the programmed timestamp.
            - register_timestamp: epoch value of the simulation time when the
                PatternTimer object is registered.
            - register_date_time: datetime object of the simulation time when
                PatternTimer object is registered.
        """
        self._pattern_generator = pattern_generator
        if register_datetime is not None:
            if register_timestamp is not None:
                raise ValueError("Both register_timestamp and "
                                 "register_timestamp can not be set at the same"
                                 " time")
            self.regiter_datetime(register_datetime)
        else:
            self.register_time(register_timestamp)
    
    def regiter_datetime(self, the_datetime):
        """Sets the current time stamp and time of registration of the generator
        to the datetime object the_datetime."""
        self.register_time(TimeController.get_epoch(the_datetime))
    
    def register_time(self, timestamp):
        """Sets the current time stamp and time of registration of the generator
        to the epoch time timestamp."""
        self._register_timestamp = timestamp
        self._current_timestamp = timestamp  
    
    def is_it_time(self, current_timestamp):
        """Returns a number that represents the number of times that the
        internal pattern generator should be triggered according to
        current_timestamp.
        Args:
            - current_timestamp: epoch value representing current simulation
                time."""
        self._current_timestamp=current_timestamp
        return 0
    def do_trigger(self, create_time):
        """Triggers the internal pattern generator as many times as create_time
        indicates through is_it_time.
        Args:
            - create_time: epoch value representing current simulation time. 
        """
        call_count = self.is_it_time(create_time)
        count=0
        while call_count>0:
            for i in range(call_count):
                count+= self._pattern_generator.do_trigger(create_time)
            call_count=0
            if self.do_reentry():
                call_count = self.is_it_time(create_time)
        return count
    
   
    def can_be_purged(self):
        """Returns True if this timer should not be called in the future again,
        False otherwise."""
        return True
    
    def do_reentry(self):
        return False
    
    

class WorkflowGenerator(PatternGenerator):
    """ Class that submits a workflow in manifest format. It receives a
    list of manifests and probabilities. Every time that it is time to submit
    it will map a random number over those probabilities and submit the
    corresponding manifest.
    """
    def __init__(self,manifest_list, share_list, workload_generator=None):
        """ Constructor
        Args:
            - manifest_list: list of file routes pointing to manifests.
            - share_list: list of floats. they must add 1.0. They represent the
                chance for a manifest in the same position in manifest_list to 
                be submitted. Must have the length as manifest_list.
            - workload_generator: orkloadGenerator object in which this
                PatternGenereator is registered.  
        """
        super(WorkflowGenerator,self).__init__(workload_generator)
        if len(manifest_list)!=len(share_list):
            raise ValueError("manifest_list and share_list must have the same"
                             " length.")
        self._manifest_selector = RandomSelector(workload_generator._random_gen)
        self._manifest_selector.set(share_list, manifest_list)
        self._workflow_count = 0
    
    def do_trigger(self, create_time):
        """Selects a manifest from self_manifest list. Selection is random
        weight by the values at self._share_list. Submits a single job
        including the manifest.
        """
        self._workflow_count+=1
        manifest  = self._manifest_selector.get_random_obj()
        manifest_name = manifest+"-"+str(self._workflow_count)
        cores, runtime  = self._parse_manifest(manifest)
        self._workload_generator.add_job(submit_time=create_time,
                                         duration=runtime,
                                         wclimit=int(runtime/60),
                                         cores=cores, 
                                         workflow_manifest=manifest_name)
        
        return 1
    
    def _parse_manifest(self, manifest_route):  
        """ Reads the manifest content from a file and extracts the 
        total number of cores and runtime.""" 
        folder=""
        try:
            from orchestration import ExperimentRunner
            folder=ExperimentRunner.get_manifest_folder()
        except:
            pass
        f = open(os.path.join(folder,manifest_route), "r")
        manifest=json.load(f)
        cores = manifest["max_cores"]
        runtime  = manifest["total_runtime"]
        f.close()
        return cores, runtime
    
    
class WorkflowGeneratorSingleJob(WorkflowGenerator):
    """Workflow generator that injects workflows as a single Job with manifest.
    """
     
    def do_trigger(self, create_time):
        """Selects a manifest from self_manifest list. Selection is random
        weight by the values at self._share_list. Submits a single job
        but the manifest is not included.
        """
        self._workflow_count+=1
        manifest  = self._manifest_selector.get_random_obj()
        cores, runtime  = self._parse_manifest(manifest)
        from stats.workflow import WasteExtractor
        we = WasteExtractor(manifest)
        """
        We need the real runtime to keep track of the core hours generated
        """
        stamps, waste_list, acc_waste = we.get_waste_changes(0)
        real_runtime=stamps[-1]
        manifest_name = manifest+"-"+str(self._workflow_count)
        self._workload_generator.add_job(submit_time=create_time,
                                         duration=real_runtime,
                                         wclimit=int(runtime/60),
                                         cores=cores, 
                                         workflow_manifest=manifest_name)
        return 1
    
class WorkflowGeneratorMultijobs(WorkflowGenerator): 
    """Workflow generator that injects workflows as a group of interdependent
    jobs."""
    
    def do_trigger(self, create_time):
        """Selects a manifest from self_manifest list. Selection is random
        weight by the values at self._share_list. Submits a single job
        but the manifest is not included.
        """
        self._workflow_count+=1
        workflow_file=self._manifest_selector.get_random_obj()
        from orchestration.running import ExperimentRunner
        workflow_route = path.join(ExperimentRunner.get_manifest_folder(),
                                  workflow_file)
        
        return self._expand_workflow(
                         workflow_route,
                         create_time, workflow_file)
        
        
    def _expand_workflow(self, manifest_route, create_time, manifest_name):
        """Submits all the tasks of the workflow as individual jobs, but
        with depencies according to the dag in the manifest.
        Args:
            - manifest: file route pointing to a json manifest file containing
                a workflow description.
            - create_time: submit time of the workflow (and this all the jobs).
        Returns: the number of jubs submitted.
        """
        cores, runtime, tasks =  WorkflowGeneratorMultijobs.parse_all_jobs(
                                                                manifest_route)
        job_count = 0
        remaining_tasks = list(tasks.values())
        while (remaining_tasks):
            new_remaining_tasks  = []
            for task in remaining_tasks:
                if self._task_can_run(task):
                    job_count+=1
                    manifest_field=self._get_manifest_field_for_task(
                            manifest_name, task["id"], self._workflow_count,
                            task["dependencyFrom"])
                    job_id=self._workload_generator.add_job(
                                        submit_time=create_time,
                                         duration=task["runtime_sim"],
                                         wclimit=int(task["runtime_limit"]/60),
                                         cores=int(task["number_of_cores"]), 
                                         workflow_manifest=manifest_field,
                                         dependency=self._gen_deps(task))
                    task["job_id"]=job_id
                else:
                    new_remaining_tasks.append(task)
            remaining_tasks=new_remaining_tasks
        return job_count
    
    def _get_manifest_field_for_task(self, manifest, 
                                     stage_name,
                                     first_job_id,
                                     deps):
        field= "|wf_{0}-{1}_{2}".format(manifest, first_job_id, stage_name)
        if deps:
            field+="_{0}".format("-".join(["d"+i["id"] for i in deps]))
        return field
        
        
              
    def _task_can_run(self, task):
        if len(task["dependencyFrom"])==0:
            return True
        for task_dep in task["dependencyFrom"]:
            if not "job_id" in list(task_dep.keys()):
                return False
        return True
    def _gen_deps(self, task): 
        dep_string = ""
        dependenciesFrom = task["dependencyFrom"]
        for dep in dependenciesFrom:
            if dep_string!="":
                dep_string+=","
            dep_string+="afterok:"+str(dep["job_id"])
        return dep_string    
    
    @classmethod    
    def parse_all_jobs(self, manifest_route):
        f = open(manifest_route, "r")
        manifest=json.load(f)
        f.close()
        
        cores = manifest["max_cores"]
        runtime  = manifest["total_runtime"]
        tasks = manifest["tasks"]
        tasks = {x["id"]: x for x in tasks}
        for task in list(tasks.values()):
            task["dependencyFrom"] = []
            task["dependencyTo"] = []
        dot_graph = pgv.AGraph(string=manifest["dot_dag"])
        for edge in dot_graph.edges():
            orig = edge[0]
            dest = edge[1]
            tasks[orig]["dependencyTo"].append(tasks[dest])
            tasks[dest]["dependencyFrom"].append(tasks[orig])
            
        return cores, runtime, tasks
        

        
            

class MultiAlarmTimer(PatternTimer):
    """ PatternTimer class that allows to program a list of future
    timestamps as alarms. It controls the current timestamp, which is
    updated by the register_time and is_it_time calls. """ 
    
    def __init__(self,pattern_generator, register_timestamp=None,
                 register_datetime=None):
        """Constructor: registerer_timestamp and register_datetime can not be
        set at the same time. 
        Args:
            - pattern_generator: PatternGenerator object to be triggered when
               an alarm is due.
            - register_timestamp: epoch timestamp as an integer pointing
                at the moment of the trace where the timer is registered.
            - register_datetime: datetime pointing at the moment of the trace
                where the timer is registered.
        """
        super(MultiAlarmTimer, self).__init__(pattern_generator,
                                              register_timestamp,
                                              register_datetime)
        self.set_alarm_list([])
        
    def set_alarm_list_date(self, datetime_list):
        """ Sets a list of future timestamps when alarm should be triggered.
        Args:
           - alarm_list: growing numeric datetime when the alarm should
               be triggered. Raises ValueError if any value is smaller than
               current timestamp or values not ordered.
            """
        self.set_alarm_list([TimeController.get_epoch(x) 
                             for x in datetime_list])
    
    def set_alarm_list(self, alarm_list):
        """ Sets a list of future timestamps when alarm should be triggered.
        Args:
           - alarm_list: growing numeric epoch timestamps when the alarm should
               be triggered. Raises ValueError if any value is smaller than
               current timestamp or values not ordered.
        """
        prev_alarm=None
        for alarm in alarm_list:
            if (self._current_timestamp is not None and 
                alarm < self._current_timestamp):
                raise ValueError("One of the alarm values is past DUE: {0}"
                                 "".format(alarm))
            if prev_alarm is not None and prev_alarm>alarm:
                raise ValueError("Alarm list should be ascending ordered")
            prev_alarm=alarm
        self._alarm_list = alarm_list
        
    def set_delta_alarm_list(self, delta_list):
        """ Sets a list of future alarms calculated by the current time stamp
        plus the seconds deltas in delta_list
        Args:
            - delta_list: list of increasing time deltas. Raises ValueError if
                not increasing.
        """
        if self._current_timestamp is None:
            raise ValueError("Delta alarms cannot be used until current"
            " timestamp has been set")
        prev_delta=None
        alarm_list = []
        for delta in delta_list:
            if prev_delta!=None and delta<prev_delta:
                raise ValueError("Deltas have to increase")
            alarm_list.append(self._current_timestamp+delta)
            prev_delta = delta
        self.set_alarm_list(alarm_list)
          
    def is_it_time(self, current_timestamp):
        """Returns true if current_timestamp>=any of the registered
        alarms."""
        super(MultiAlarmTimer, self).is_it_time(current_timestamp)
        pos=0
        for alarm in self._alarm_list:
            if current_timestamp >= alarm:
                pos+=1
            else:
                break
        self.set_alarm_list(self._alarm_list[pos:])
        return pos

    def can_be_purged(self):
        return len(self._alarm_list)==0
            
class RepeatingAlarmTimer(PatternTimer):  
    """Extension of MultiAlarmTimer which always triggers an alarm once every
    period seconds
    """
    
    def set_alarm_period(self, period):
        """Sets the repeating alarm period in seconds.
        Args:
            - period: number of seconds betweewn two alarms. When called the
                first time, alarm will be triggered period seconds in the
                future.
        """
        self._alarm_period = period   
        self._last_timestamp=self._current_timestamp       
        
    def is_it_time(self, current_timestamp):
        """Returns true if current_time is > last_alarm_time+period for the
        first time. 
        """
        adjusted_ct = (int(current_timestamp/self._alarm_period) 
                       * self._alarm_period)
        adjusted_lt = (int(self._last_timestamp/self._alarm_period+1) 
                       * self._alarm_period)
        
        if (current_timestamp==self._last_timestamp 
            or not (adjusted_ct >= adjusted_lt)):
            return 0
        self._last_timestamp=current_timestamp
        trigger_count = (adjusted_ct-adjusted_lt)/self._alarm_period + 1
        return trigger_count
    def can_be_purged(self):
        return False
