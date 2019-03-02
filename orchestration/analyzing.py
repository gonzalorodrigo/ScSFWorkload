import numpy as np
from orchestration.definition import ExperimentDefinition
from stats.compare import WorkflowDeltas
from stats.trace import ResultTrace


class AnalysisRunnerSingle(object):
    """Class to run an analysis on the results of a single experiment. This is
    base class for other analyses that may include more than one trace."""
    def __init__(self, definition):
        """Constructor.
        Args:
        - definition: Definition object containing the configuration of the
            experiment to be analyzed by this object.
        """
        self._definition=definition
        
    def load_trace(self, db_obj):
        """Reads and returns the experiment trace from analysis database.
         Args:
        - db_obj: DB object configured to access the analysis database."""
        result_trace = ResultTrace()
        result_trace.load_trace(db_obj, self._definition._trace_id, False)
        return result_trace
    
    def do_full_analysis(self, db_obj):
        """Do job and workflow variables analysis: CDF and numeric. Also does
        utilization analysis. Stores results in the database.
         Args:
        - db_obj: DB object configured to access the analysis database.
        """
        print(("Analyzing trace:", self._definition._trace_id))
        result_trace = self.load_trace(db_obj)
        
        result_trace.calculate_job_results(True, db_obj, 
                                       self._definition._trace_id,
                                       start=self._definition.get_start_epoch(),
                                       stop=self._definition.get_end_epoch())
        result_trace.calculate_job_results_grouped_core_seconds(
                       self._definition.get_machine().get_core_seconds_edges(),
                       True, db_obj, 
                       self._definition._trace_id,
                       start=self._definition.get_start_epoch(),
                       stop=self._definition.get_end_epoch())
        workflows=result_trace.do_workflow_pre_processing()
        if len(workflows)>0:
            result_trace.calculate_workflow_results(True, db_obj, 
                                       self._definition._trace_id,
                                       start=self._definition.get_start_epoch(),
                                       stop=self._definition.get_end_epoch())
        result_trace.calculate_utilization(
                            self._definition.get_machine().get_total_cores(),
                            do_preload_until=self._definition.get_start_epoch(),
                            endCut=self._definition.get_end_epoch(),
                            store=True, db_obj=db_obj,
                            trace_id=self._definition._trace_id)
        self._definition.mark_analysis_done(db_obj)

    def do_workflow_limited_analysis(self, db_obj, num_workflows):
        """Dows the workflow analysis on the trace only for the first
        num_workflows workflows."""
        result_trace = self.load_trace(db_obj)
        workflows=result_trace.do_workflow_pre_processing()
        if len(workflows)>0:
            result_trace.truncate_workflows(num_workflows)
            result_trace.calculate_workflow_results(True, db_obj, 
                                       self._definition._trace_id,
                                       start=self._definition.get_start_epoch(),
                                       stop=self._definition.get_end_epoch(),
                                       limited=True)
        self._definition.mark_second_pass(db_obj)

class AnalysisGroupRunner(AnalysisRunnerSingle):
    """Class to run analyses on experiments that are groups of experiments: e.g.
    10 repetitions of the same conditions with different random seeds. Job and
    workflow variables are calculated together. Median utilization is
    calculated.
    """
    def load_trace(self, db_obj):
        result_trace = ResultTrace()
        return result_trace
        
    
    def do_full_analysis(self, db_obj):
        result_trace = self.load_trace(db_obj)
        first=True
        last=False
        for trace_id in self._definition._subtraces:
            last=trace_id==self._definition._subtraces[-1]
            result_trace.load_trace(db_obj, trace_id)
            result_trace.do_workflow_pre_processing(append=not first)
            one_definition  = ExperimentDefinition()
            one_definition.load(db_obj, trace_id)
            result_trace.fill_job_values(
                                    start=one_definition.get_start_epoch(),
                                    stop=one_definition.get_end_epoch(),
                                    append=not first)
            result_trace.fill_workflow_values(
                                    start=one_definition.get_start_epoch(),
                                    stop=one_definition.get_end_epoch(),
                                    append=not first)
            result_trace.calculate_job_results_grouped_core_seconds(
                       one_definition.get_machine().get_core_seconds_edges(),
                       last, db_obj, 
                       self._definition._trace_id,
                       start=one_definition.get_start_epoch(),
                       stop=one_definition.get_end_epoch(),
                       append=not first)
            first=False
            
        result_trace.calculate_and_store_job_results(store=True,
                                        db_obj=db_obj,
                                        trace_id=self._definition._trace_id)
        result_trace._wf_extractor.calculate_and_store_overall_results(store=True, 
                                        db_obj=db_obj,
                                        trace_id=self._definition._trace_id)
        result_trace._wf_extractor.calculate_and_store_per_manifest_results(
                     store=True,
                     db_obj=db_obj,
                     trace_id=self._definition._trace_id)
        
        result_trace.calculate_utilization_median_result(
                                self._definition._subtraces,
                                store=True, 
                                db_obj=db_obj,
                                trace_id=self._definition._trace_id)
        result_trace.calculate_utilization_mean_result(
                                self._definition._subtraces,
                                store=True, 
                                db_obj=db_obj,
                                trace_id=self._definition._trace_id)
        self._definition.mark_analysis_done(db_obj)     
    
    def do_only_mean(self, db_obj):
        result_trace = self.load_trace(db_obj)  
        result_trace.calculate_utilization_mean_result(
                                self._definition._subtraces,
                                store=True, 
                                db_obj=db_obj,
                                trace_id=self._definition._trace_id)
        
    def do_workflow_limited_analysis(self, db_obj, workflow_count_list):
        result_trace = self.load_trace(db_obj)
        if len(workflow_count_list)!=len(self._definition._subtraces):
            raise Exception("Number of subtraces({0}) is not the samas the"
                            " limit on workflow count({1})".format(
                              len(workflow_count_list),
                              len(self._definition._subtraces)))
        first=True
        acc_workflow_count=0
        for (trace_id, workflow_count, subt) in zip(self._definition._subtraces,
                                              workflow_count_list,
                                              list(range(len(workflow_count_list)))):
            """
            For each sub trace, we need the other two.
                - we looad all the jobs.
                - do a pre-process.
                - new workflows, we append a number to the workflow count to
                    separete workflows across subtraces-
                - we count how many workflows there should be until now.
                - we truncate.
            
            we make sure that the workflow names are restored (eliminating a
            toek that allows to see if a workflow is new or not).
            
            We do the "limited processing of the worklfow"
            """
            acc_workflow_count+=workflow_count
            result_trace.load_trace(db_obj, trace_id)
            result_trace.do_workflow_pre_processing(append=not first,
                                                    do_processing=False)
            result_trace.rename_workflows(subt)
            """ THe rename call ensures that the workflows are ordered such as
            we only turncate the ones from the last subtrace."""
            result_trace.truncate_workflows(acc_workflow_count)
            first=False
        result_trace.rename_workflows(None)
        result_trace._wf_extractor.do_processing()
        print(("After FINAL number of WFs",
               len(list(result_trace._wf_extractor._workflows.values()))))
        result_trace.calculate_workflow_results(True, db_obj, 
                                       self._definition._trace_id,
                                       start=self._definition.get_start_epoch(),
                                       stop=self._definition.get_end_epoch(),
                                       limited=True)
       
        self._definition.mark_second_pass(db_obj)         

class AnalysisRunnerDelta(AnalysisRunnerSingle):
    """Class to run analyses on delta experiments: Calculates statistics on the
    difference between values for the same workflows accroos traces with the
    same seed but different scheduling policies.
    """
    def load_trace(self, db_obj):
        
        trace_comparer = WorkflowDeltas()
        pairs = list(zip(self._definition._subtraces[0::2],
                    self._definition._subtraces[1::2]))
        for (first_id, second_id) in pairs:
            trace_comparer.load_traces(db_obj, first_id, second_id)
            trace_comparer.produce_deltas(True)
        return trace_comparer
        
    def do_full_analysis(self, db_obj):
        trace_comparer = self.load_trace(db_obj)
        trace_comparer.calculate_delta_results(True, db_obj, 
                                               self._definition._trace_id)
        self._definition.mark_analysis_done(db_obj)
        
        
        

