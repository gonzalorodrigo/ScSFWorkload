from stats.workflow import TaskTracker
from generate.pattern import WorkflowGeneratorMultijobs
from os import path
from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace

class StartTimeCorrector(object):
    
    def __init__(self):
        self.manifest_dics = {}
        
    def correct_times(self, db_obj, trace_id):
        self._experiment = ExperimentDefinition()
        self._experiment.load(db_obj, trace_id)
        
        self._trace = ResultTrace()
        print "Loading trace {0}".format(trace_id)
        self._trace.load_trace(db_obj, trace_id)
        trace_type = self._experiment._workflow_handling
        print "Calculating corrected start times for trace {0}".format(trace_id)
        modified_start_times = self.get_corrected_start_times(trace_type)
        print ("Found {0} jobs which start time was 0, but had ended.".format(
                                            len(modified_start_times)))
        print ("About to update times")
        self.apply_new_times(db_obj, modified_start_times)    
    
    def apply_new_times(self, db_obj, modified_start_times):
        trace_id=self._experiment._trace_id
        for id_job in modified_start_times.keys():
            time_start=modified_start_times[id_job]
            print ("updating trace_id({0}), id_job({1}) with time_start: {2}"
                   "".format(trace_id, id_job, time_start))
            self.update_time_start(db_obj,trace_id, id_job, time_start)
    
    def update_time_start(self, db_obj, trace_id, id_job, time_start):
        """
        query =("update traces set time_start={0} where trace_id={1} and "
                " id_job={2}",format(time_start, trace_id, id_job))
        """
        db_obj.setFieldOnTable(
                               "traces",
                               "time_start", str(time_start),
                               "id_job", str(id_job), 
                    extra_cond="and trace_id={0}".format(trace_id),
                    no_commas=True)
    def get_corrected_start_times(self, trace_type):
        modified_start_times = {}
        
        for (id_job, job_name, time_submit, time_start, time_end) in zip(
                    self._trace._lists_submit["id_job"],
                    self._trace._lists_submit["job_name"],
                    self._trace._lists_submit["time_submit"],
                    self._trace._lists_submit["time_start"],
                    self._trace._lists_submit["time_end"]):
            if time_start==0 and time_end!=0 and "wf" == job_name[:2]:
                modified_start_times[id_job]=self.get_time_start(job_name,
                                                              time_end,
                                                              trace_type)

        return modified_start_times

        
    def get_workflow_info(self, workflow_file):
        
        
        if not workflow_file in self.manifest_dics.keys():
            from orchestration.running import ExperimentRunner
            manifest_route = path.join(ExperimentRunner.get_manifest_folder(),
                                      workflow_file)
            cores, runtime, tasks =  WorkflowGeneratorMultijobs.parse_all_jobs(
                                                                manifest_route)
            self.manifest_dics[workflow_file] = {"cores": cores, 
                                            "runtime":runtime, 
                                            "tasks":tasks}
        
        return self.manifest_dics[workflow_file]
        
            
        
    def get_time_start(self, job_name, time_end, trace_type="manifest"):
        name, stage_id, deps = TaskTracker.extract_wf_name(job_name)
        workflow_file="-".join(name.split("-")[:-1])
        from orchestration import ExperimentRunner
        manifest_info = self.get_workflow_info(workflow_file)
        (cores, runtime, tasks) =  (manifest_info["cores"],
                                    manifest_info["runtime"],
                                    manifest_info["tasks"])
        if stage_id is "":
            if trace_type == "multi":
                raise SystemError("Found a bounding job ({0}) in a "
                                  "dependencies type trace.".format(job_name))
            if trace_type == "manifest":
                return time_end
            else:
                return time_end-runtime 
        else:
            return time_end-int(tasks[stage_id]["runtime_sim"])
        
    @classmethod
    def get_traces_with_bad_time_starts(cls, db_obj):
        query = """
                SELECT traces.trace_id tid, name, count(*) cc
                FROM traces, experiment
                WHERE traces.trace_id=experiment.trace_id 
                     AND time_start=0 AND time_end!=0 
                     AND job_name!="sim_job"
                group by traces.trace_id
        """
        result_list=db_obj.doQueryDic(query)
        trace_id_list = [res["tid"] for res in result_list]
        return trace_id_list
    