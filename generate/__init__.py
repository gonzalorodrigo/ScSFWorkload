""" 
Functions and code generate workloads
"""
from analysis import ProbabilityMap
import bisect
import datetime
import random
import random_control



class TimeController(object):
    """
    The Time controller class controls the point in time in which a trace
    generation process is. It also produces inter arrival times according to
    characteristics of a random variable.  
    """

    def __init__(self, inter_arrival_gen):
        """
        Args:
            - inter_arrival_gen: A configured probability map. it's 
                configuration will affect the inter arrival time values
                produced by this object.
        """
        self._inter_arrival_generator = inter_arrival_gen
        self._time_counter = 0
        self._start_date = 0
        self._run_limit = 0
        self._max_interrival=None
    
    def set_max_interarrival(self, max_value):
        self._max_interrival=max_value
        
    def reset_time_counter(self, start_date_time=None):
        """Sets the trace start and current time counters.
            Args: 
            - start_date_time: If set to a datetime.datetime object its epoch
                value will be used as start and current trace time. if set to
                None, start and current counters are set to now's epoch time.        
        """
        if start_date_time is None:
            start_date_time = datetime.datetime.now()
        self._time_counter = TimeController.get_epoch(start_date_time)
        self._start_date = self._time_counter
    def set_run_limit(self, seconds):
        """Sets the time running limit of the trace as _star_date+_run_limit"""
        self._run_limit = seconds
        
    def is_time_to_stop(self):
        """True if the interntal time counter is over the end time"""
        return self._time_counter-self._start_date > self._run_limit
    
    def get_next_job_create_time(self):
        """Returns a job inter arrival time according tot he characteristics
        of the configure ProbabilityMap. It also increasses the internal
        time counter with the returned value"""
#         step = None
#         while (step is None
#                or self._max_interrival is not None 
#                and self._max_interrival<step):
#             step = self._inter_arrival_generator.produce_number()
        step = self._inter_arrival_generator.produce_number()
        if self._max_interrival is not None:
            step=min(self._max_interrival, step)
        self._time_counter=float(self._time_counter)+float(step)
        return int(self._time_counter)
    
    def get_current_time(self):
        """Returns current time"""
        return self._time_counter
    def get_runtime(self):
        return self._time_counter - self._start_date
    @classmethod
    def get_epoch(cld, the_datetime):
        return int(the_datetime.strftime('%s'))
    
        
    
class WorkloadGenerator(object):
    """
    WorkloadGenerator generates job traces to be run in a scheduler simulator
    for a particular system with its workload characteristics, users, and
    systems and queue configuarions.
    
    WorkloadGenerator uses a machine configuration (Machine class), and trace
    generator (TraceGenerator class) to calculate a number of jobs and dump
    them in a trace format. The machine configuration describes jobs'
    random variables inter-arrival time, estimated wallclock, estimated
    wallclock accuracy, allocated cores. The trace generator is resposinble
    of translating the jobs information into an specific format accepted by
    a simulator.
    
    The generator needs other extra information required by the slurm batch
    system: user names in the system, names of the QOS policies, and names of
    the present partitions and accounts. A job's user, qos, partition, and 
    account are randomly (uniform) chosen from these lists. To change
    this behavior re-define _get_user, _get_partition, _get_account, and 
    _get_qos
    """
    def __init__(self, machine, trace_generator, user_list, qos_list,
                 partition_list, account_list):
        """
        Creation Method
        Args:
            - machine: Machine class modeling the job random variables.
            - trace_generator: TraceGenerator class that will store the 
                resulting traces when the save_trace method is invoked.
            - user_list: list of non repeating strings containing the users
                to be present in the job trace.
            - qos_list: list of non repeating strings containing the qos
                policies to be present in the job trace.
            - account_list: list of non repeating strings containing the
                accounts to be repsent int he job trace 
        """
        self._machine = machine
        self._trace_generator = trace_generator
        self._time_controller = TimeController(
                                        machine.get_inter_arrival_generator())
        self._job_id_counter=1
        self._user_list = user_list
        self._qos_list = qos_list
        self._partition_list=partition_list
        self._account_list=account_list
        
        self._random_gen = random_control.get_random_gen()

        from generate.special.workflow_percent import WorkflowPercent

        self._workload_selector = WorkflowPercent(self._random_gen,
                                                  trace_generator,
                                                  self._time_controller,
                                                  machine.get_total_cores())
        self._workload_selector.set_remaining(self)
        
        self._pattern_timers = []
        
        self._filter_func = None
        self._filter_cores = None
        self._filter_runtime = None
        self._filter_core_hours = None
        self._disable_generate_workload_element=False
    
    def generate_trace(self, start_date_time, run_time_limit, job_limit=None):
        """
        Generates and stores in the jobect a job trace starting at
        start_date_time, spanning run_time, with at most job_limit jobs.
        Args:
            - start_date_time: datetime.datetime timestamp where the trace
                should start.
            - run_time_limit: integer seconds that the trace should run.
            - job_limit: if set with a number, trace will not contai more than
                job_limit jobs.
        """
        self._time_controller.reset_time_counter(start_date_time)
        self._time_controller.set_run_limit(run_time_limit)
        self.set_all_time_controllers();
        
        jobs_generated=0
        
        while (not self._time_controller.is_time_to_stop()):
            create_time=self._time_controller.get_next_job_create_time()
            if (job_limit!=None and jobs_generated>=job_limit):
                break
            if not self._disable_generate_workload_element:
                jobs_generated+=self._generate_workload_element(create_time)
            jobs_generated+=self._pattern_generator_timers_trigger(create_time)
        print("{0} jobs generated".format(jobs_generated))
        return jobs_generated
    
    def disable_generate_workload_elemet(self):
        self._disable_generate_workload_element=True
    
    def _generate_workload_element(self, create_time):
        workload_obj = self._workload_selector.get_random_obj()
        if workload_obj is None:
            return 0
        if workload_obj == self:
            self._generate_new_job(create_time)
            return 1
        return workload_obj.do_trigger(create_time)
        
    
    def save_trace(self, file_route):
        """ Saves required info to run a scheduler simulation: list of jobs,
        list of users and list of qos policies present."""
        self._trace_generator.dump_trace(file_route+".trace")
        self._trace_generator.dump_users(file_route+".users")
        self._trace_generator.dump_qos(file_route+".qos")
    
    def set_max_interarrival(self, max_interarrival):
        self._time_controller.set_max_interarrival(max_interarrival)
    
    def config_filter_func(self, filter_func):
        self._filter_func=filter_func
        
        
    def _job_pass_filter(self, cores, runtime):
        """Jobs that are too big should be filtered."""
        if self._filter_func is None:
            return True
        else:
            return self._filter_func(cores, runtime)
        
    def _generate_new_job(self, submit_time, cores=None,run_time=None,
                          wc_limit=None, override_filter=False):
        """Uses the _machine to calculate the characteristics for a new job and
        stores them in the _trace_generator."""
        self._job_id_counter+=1
        user=self._get_user()
        qos=self._get_qos()
        partition=self._get_partition()
        account=self._get_account()
        cores_v, wc_limit_v, run_time_v = self._machine.get_new_job_details()
        if cores is None:
            cores=cores_v
        if wc_limit is None:
            wc_limit=wc_limit_v
        if run_time is None:
            run_time=run_time_v
        
        if not override_filter and not self._job_pass_filter(cores, run_time):
            return -1
        return self.add_job(self._job_id_counter, user, submit_time,
                                      run_time, wc_limit,cores, 
                                      qos, partition, account,
                        reservation="", dependency="", workflow_manifest="|")
        
    def add_job(self, job_id=None, username=None, submit_time=None,
                        duration=None, wclimit=None, cores=None,
                        qosname=None, partition=None, account=None,
                        reservation=None, dependency=None,
                        workflow_manifest=None):
        """Inserts a job in the trace according to the input parameters, if
        any of them is not set, they are set to system configured values."""
        
        it_is_a_workflow=(workflow_manifest and len(workflow_manifest)>1)
        
        if job_id is None:
            self._job_id_counter+=1
            job_id=self._job_id_counter
        if username is None:
            username=self._get_user(no_random=it_is_a_workflow)
        if qosname is None:
            qosname=self._get_qos(no_random=it_is_a_workflow)
        if partition is None:
            partition=self._get_partition(no_random=it_is_a_workflow)
        if account is None:
            account=self._get_account(no_random=it_is_a_workflow)
        if submit_time is None:
            submit_time = self._time_controller.get_current_time()
         
        if not cores or not wclimit or not duration:
            cores_pre, wc_limit_pre, run_time_pre = (       
                                            self._machine.get_new_job_details())
        if cores is None:
            cores = cores_pre
        if wclimit is None:
            wclimit = wc_limit_pre
        if duration is None:
            duration = run_time_pre
        
        if reservation is None:
            reservation = ""
        if dependency is None:
            dependency = ""
        
        cores_s=None
        cores_s_real=None
        if workflow_manifest is None:
            workflow_manifest = ""
        else:
            if workflow_manifest[0]!="|":
                from stats.workflow import WasteExtractor
                manifest_file=workflow_manifest.split("-")[0]
                we = WasteExtractor(manifest_file)
                stamps, waste_list, acc_waste = we.get_waste_changes(0)
                cores_s_real=(min(wclimit*60,duration) *cores)
                cores_s=cores_s_real-acc_waste                
        
        
        
        self._trace_generator.add_job(job_id, username, submit_time,
                                      duration, wclimit,cores,
                                      1,self._machine._cores_per_node,
                                      qosname, partition, account,
                                      reservation=reservation,
                                      dependency=dependency,
                                      workflow_manifest=workflow_manifest,
                                      cores_s=cores_s,
                                      ignore_work=False,
                                      real_core_s=cores_s_real)
        return job_id
        
    def _get_user(self, no_random=False):
        return self._get_random_in_list(self._user_list, 
                                        no_random=no_random)
    
    def _get_qos(self, no_random=False):
        return self._get_random_in_list(self._qos_list,
                                        no_random=no_random)
    
    def _get_partition(self, no_random=False):
        return self._get_random_in_list(self._partition_list,
                                        no_random=no_random)
    
    def _get_account(self, no_random=False):
        return self._get_random_in_list(self._account_list,
                                        no_random=no_random)
    
    def _get_random_in_list(self, value_list,
                            no_random=False): 
        if len(value_list)==1 or no_random:
            return value_list[0]
        pos=self._random_gen.randint(0, len(value_list)-1)
        return value_list[pos]
    
    def _pattern_generator_timers_trigger(self, timestamp):
        """ Triggers registered pattern timers with timestamp. Purge those that
        are done.
        """
        count = 0
        new_pattern_timers = []
        for timer in self._pattern_timers:
            count += timer.do_trigger(timestamp)
            if not timer.can_be_purged():
                new_pattern_timers.append(timer)
        
        self._pattern_timers= new_pattern_timers
        return count
            
    
    def register_pattern_generator_timer(self,timer):
        """Registers an workload pattern generator that is driven
        by time deltas controlled by PatternTimer object. The time
        will receive calls to is_it_time(), and when it returns true
        pattern_generator.do_trigger() will be called.
        
        Args:
            - timer: PatternTimer object that will govern when the 
        """
        self._pattern_timers.append(timer)
    
    def set_all_time_controllers(self):
        for timer in self._pattern_timers:
            timer.register_time(self._time_controller.get_current_time())
                
    
    def register_pattern_generator_share(self, pattern_generator,
                                         share):
        """Registers a workload pattern generator that will be called %share of
        times of the job creation times driven by the programmed inter-arrival
        time.
        
        Args: 
            - pattern_generator: PatternGenerator object that will be invoked to
                produce jobs.
            - share: positive float in the range [0.0, 100000]. It is a composed
                value: YYYXX.xx. XXX.xx represent the share corresponding to 
                registered pattern generator. If the YYY is present, a workload
                cap is configured and no more core-hours than YYY/1*runtime
                will be added.
        """ 
        if share<0.0 or share>100000.0:
            raise ValueError("Share has to be in the range [0.0, 100000.0]: "
                             "{0}".format(share))
        actual_share=share
        upper_cap=None
        if actual_share>1:
            upper_cap=float(int(actual_share))/1000
            actual_share-=upper_cap*1000
        self._workload_selector.add_obj(pattern_generator, actual_share)
        if upper_cap is not None:
            self._workload_selector.config_upper_cap(upper_cap)
    
        
class RandomSelector(object):
    """
    Random selector is an object that allows to store a list of objects with an
    associated probability to each, and then extract a random object according
    to that probability. There are to type of objects:
    - Remaining one, selected if the rest are not selected.
    - Added objects: selected if the probability number fits in its share.
    """
    def __init__(self, random_gen):
        """Constructor.
        Args:
            - random_gen: is a Random generator object that has a
                uniform(float, float) method that returns a float 
                between the two.
        """
        self._random_gen = random_gen
        self._share_list=[]
        self._obj_list=[]
        self._prob_list=[]
        self._remaining_obj = None
    def set(self, share_list, obj_list, remaining_obj=None):
        """ Sets the internal list of shares, objects and remaining obj
        Args:
            - share_list: list of probabilities associated to each object.
                they should not sum more than 1.0 and have the same number of
                elements than obj_list. If remaining obj is not set, the
                shares must add 1.0
            - obj_list: list of objects associated to a probability.
            - remaining_obj: If set, this object will retain the remaining
                probability.
        """
        self._share_list=share_list
        self._obj_list=obj_list
        self._remaining_obj=remaining_obj
        if remaining_obj is not None:
            self._obj_list=[self._remaining_obj] + self._obj_list
        self._recalculate()
    
    def remove_remaining(self):
        if self._remaining_obj:
            self._remaining_obj=None
            self._obj_list=self._prob_list[1:]
        self._recalculate()
    def set_remaining(self, obj):
        self.remove_remaining()
        self._remaining_obj = obj
        self._obj_list=[self._remaining_obj] + self._obj_list
        self._recalculate()
        
    def add_obj(self, obj, share):
        if not self._remaining_obj:
            raise ValueError("Add cannot be used with RandomSelector that  has "
                             "no remaining_obj")
        self._obj_list.append(obj)
        self._share_list.append(share)
        self._recalculate()
        
    def _recalculate(self):
        self._prob_list=[]
        prob = 0.0
        if self._remaining_obj:
            prob = 1.0
            if len(self._share_list):
                prob-=sum(self._share_list)
            self._prob_list.append(prob)
        if len(self._share_list):
            for s in self._share_list:
                prob+=s
                self._prob_list.append(prob)
        if prob>1.0:
            raise ValueError("Probabilities add more than 1.0 check "
                             "share list: {0}".format(self._share_list))
    def get_random_obj(self):
        """Gets a random obj according to configured probabilities"""
        if len(self._prob_list)==0:
            return None
        elif len(self._prob_list)==1:
            return self._obj_list[0]
        r = self._random_gen.uniform(0,1)
        pos = bisect.bisect_left(self._prob_list, r)
        return self._obj_list[pos]
     
        
            
            
    