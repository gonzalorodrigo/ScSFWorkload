from stats import calculate_results, load_results
from stats.trace import ResultTrace

class WorkflowDeltas(object):
    """ This class produces the delta in the runtime, wait time, turnaround
    time, and stretch factor for the same workflow in two different traces.
    It is meant compare the effect on the same workflow  when the scheduling
    algorithm is different. Values are compared: 
        values_from_second_trace - values_first_trace
    """
    
    def __init__(self):
        """Constructor:
        Args:
    
        """
        self._first_trace = None
        self._first_trace_id=None
        self._second_trace = None
        self._second_trace_id=None
        
        self._runtime_deltas = None
        self._waitime_deltas = None
        self._turnaround_deltas = None
        self._stretch_deltas = None
        self._wf_names = None
    
    def load_traces(self, db_obj, first_id, second_id):
        """
        Loads the jobs from the two traces to compare.
        Args:
        - first_id: int, trace_id of the first trace
        - second_id: int, trace_id of the seconds trace
        """
        
        self._first_trace = ResultTrace()
        self._first_trace_id=first_id

        self._second_trace = ResultTrace()
        self._second_trace_id=second_id
        
        self._first_trace.load_trace(db_obj, self._first_trace_id)
        self._second_trace.load_trace(db_obj, self._second_trace_id)
        
        self._first_workflows=self._first_trace.do_workflow_pre_processing()
        self._second_workflows=self._second_trace.do_workflow_pre_processing()
    
    def produce_deltas(self, append=False):
        """
        Produces and stores the deltas between the two stored traces. 
        Args:
        - append: If true, previously captured delta values are discarded. If
            false, newly produced are added,
        Returns: current delta information.
        """
        (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas) = self._internal_produce_deltas()
        if not append or self._runtime_deltas == None:
            (self._wf_names, self._runtime_deltas, 
                 self._waitime_deltas,
                 self._turnaround_deltas,
                 self._stretch_deltas) = ([], [],
                                          [], [],
                                          [])
        
        self._wf_names+= wf_names
        self._runtime_deltas+= runtime_deltas
        self._waitime_deltas+= waitime_deltas
        self._turnaround_deltas+= turnaround_deltas
        self._stretch_deltas+= stretch_deltas
        
        return  (self._wf_names, self._runtime_deltas, 
                 self._waitime_deltas,
                 self._turnaround_deltas,
                 self._stretch_deltas)
        
    def _internal_produce_deltas(self):
        """
        Returns the list of worklfow names found in common to the two traces
        plus the lists with the deltas of each variable.
        """
        runtime_deltas = []
        waitime_deltas = []
        turnaround_deltas = []
        stretch_deltas = []
        wf_names = []
        for wf_name in self._first_workflows.keys():
            if wf_name in self._second_workflows.keys():
                wf_1=self._first_workflows[wf_name]
                wf_2=self._second_workflows[wf_name]
                runtime_d, waittime_d, turnaround_d, stretch_d = (
                          self.compare_wfs(wf_1, wf_2))
                
                runtime_deltas.append(runtime_d)
                waitime_deltas.append(waittime_d)
                turnaround_deltas.append(turnaround_d) 
                stretch_deltas.append(stretch_d)
                wf_names.append(wf_name)
        
        return  (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas)
    
    def compare_wfs(self, wf_1, wf_2):
        """Produces the diferences betwen varibles of the two workflows.
        Returns: numeric value on difference of: runtime, waittime, turnaround,
        stretch_factor.
        """
        return (wf_2.get_runtime()-wf_1.get_runtime(),
                wf_2.get_waittime()-wf_1.get_waittime(),
                wf_2.get_turnaround()-wf_1.get_turnaround(),
                wf_2.get_stretch_factor()-wf_1.get_stretch_factor())
    
    def calculate_delta_results(self, store, db_obj=None, trace_id=None):
        """
        Requires produce delta to be called at least once.
        """
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")
                 
        data_list = [self._runtime_deltas, self._waitime_deltas,
                     self._turnaround_deltas, self._stretch_deltas]
        field_list=["delta_runtime", "delta_waittime", "delta_turnaround",
                    "delta_stretch"]
        bin_size_list = [30,30, 30, 0.01]
        minmax_list = [None, None, None, None]
        return calculate_results(data_list, field_list,
                      bin_size_list,
                      minmax_list, store=store, db_obj=db_obj, 
                      trace_id=trace_id)
    
    def load_delta_results(self, db_obj, trace_id):
        """ Creates Histogram and NumericStats objects and sets them as local
        object as self._[analyzed job field] over the delta information of the 
        worfklows between to traces. The information is pulled from a database.
        Args:
        - db_obj: DBManager Object used to pull the information from a database.
        - trace_id: numeric id of the trace to which the data corresponds.
        """
        field_list=["delta_runtime", "delta_waittime", "delta_turnaround",
                    "delta_stretch"]
        return load_results(field_list, db_obj, trace_id)



        
        