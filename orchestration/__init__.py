import os
from time import sleep

from commonLib.DBManager import DB
from orchestration.analyzing import (AnalysisRunnerSingle,
                                     AnalysisRunnerDelta,
                                     AnalysisGroupRunner)
from orchestration.definition import (ExperimentDefinition,
                                      GroupExperimentDefinition,
                                      DeltaExperimentDefinition)
from orchestration.running import ExperimentRunner
from stats import  NumericStats


def get_central_db(dbName="workload"):
    """Returns a DB object configured to access the workload analysis
    central database. It gets configured through environment variables.
    Args:
    - dbName: string with database use name to use if not configured
        through environment var.
    Env vars:
    - ANALYSIS_DB_HOST: hostname of the system hosting the database.
    - ANALYSIS_DB_NAME: database name to read from.
    - ANALYSIS_DB_USER: user to be used to access the database.
    - ANALYSIS_DB_PASS: password to be used to used to access the database.
    - ANALYSIS_DB_PORT: port on which the database runs. 
    """
    return DB(os.getenv("ANALYSIS_DB_HOST", "127.0.0.1"),
               os.getenv("ANALYSIS_DB_NAME", dbName),
               os.getenv("ANALYSIS_DB_USER", "root"),
               os.getenv("ANALYSIS_DB_PASS", ""),
               os.getenv("ANALYSIS_DB_PORT","3306"))

def get_sim_db(hostname="127.0.0.1"):
    """Returns a DB object configured to access the internal database of 
    a slurm scheduler. It gets configured through environment variables.
    Args:
    - hostname: default hostname of the machine containing the database if not
        set through an env var.
    Env Vars:
    - SLURM_DB_HOST: slurm database host to connect to.
    - SLURM_DB_NAME: slurm database name of the slurm worker. If not set takes
    slurm_acct_db.
    - SLURMDB_USER: user to be used to access the slurm database.
    - SLURMDB_PASS: password to be used to used to access the slurm database.
    - SLURMDB_PORT: port on which the slurm database runs.
    """
    return DB(os.getenv("SLURM_DB_HOST", hostname),
               os.getenv("SLURM_DB_NAME", "slurm_acct_db"),
               os.getenv("SLURMDB_USER", None),
               os.getenv("SLURMDB_PASS", None),
               os.getenv("SLURMDB_PORT","3306"))
    
class ExperimentWorker(object):
    """This class retrieves experiment configurations, creates the corresponding
    workload, configures a slurm experiment runner, runs the experiment, and
    stores results in the analysis database.
    
    Configuration of the running enviroment camos from the static 
    configuration of the ExperimentRunner class.
    """ 
    def do_work(self, central_db_obj, sched_db_obj, trace_id=None):
        """
        Args:
        - central_db_obj: DB object configured to access the analysis database.
        - sched_db_obj: DB object configured to access the slurm database of
            an experiment worker. 
        - trace_id: If set to an experiment valid trace_id, it runs only the
            experiment identified by trace_id.
        """
        there_are_more=True
        while there_are_more:
            ed = ExperimentDefinition()
            if trace_id:
                ed.load(central_db_obj, trace_id)
                ed.mark_pre_simulating(central_db_obj)
            else:
                there_are_more = ed.load_fresh(central_db_obj)
            if there_are_more:
                print(("About to run exp({0}):{1}".format(
                                ed._trace_id, ed._name)))
                er = ExperimentRunner(ed)
                if(er.do_full_run(sched_db_obj, central_db_obj)):
                    print(("Exp({0}) Done".format(
                                                 ed._trace_id)))
                else:
                    print(("Exp({0}) Error!".format(
                                                 ed._trace_id)))
            if trace_id:
                break  
    
    def rescue_exp(self, central_db_obj, sched_db_obj, trace_id=None):
        """Retrieves the job trace from the database of an experiment worker and
        stores it in the central db.
        Args:
        - central_db_obj: DB object configured to access the analysis database.
        - sched_db_obj: DB object configured to access the slurm database of
            an experiment worker. 
        - trace_id: trace_id of the experiment to which the rescued trace
            corresponds.
        """
        there_are_more=True
        while there_are_more:
            ed = ExperimentDefinition()
            if trace_id:
                ed.load(central_db_obj, trace_id)
                ed.mark_simulation_done(central_db_obj)
            else:
                there_are_more = ed.load_next_state("simulation_failed",
                                                    "simulation_done")
            if there_are_more:
                print(("About to run resque({0}):{1}".format(
                                ed._trace_id, ed._name)))
                er = ExperimentRunner(ed)
                if(er.check_trace_and_store(sched_db_obj, central_db_obj)):
                    er.clean_trace_file()
                    print(("Exp({0}) Done".format(
                                                 ed._trace_id)))
                else:
                    print(("Exp({0}) Error!".format(
                                                 ed._trace_id)))
            if trace_id:
                break  

class AnalysisWorker(object):
    """This class processes the results of different experiment types and store
    the final results in the analysis database.
    """
    def do_work_single(self, db_obj, trace_id=None):
        """Processes single type experiment results.
        Args:
        - db_obj: DB object configured to access the analysis database.
        - trace_id: If set to "None", it processes all experiments in 
            "simulation_state". If set to an integer, it will analyze the
            experiment identified by trace_id.
        """
        there_are_more=True
        while there_are_more:
            ed = ExperimentDefinition()
            if trace_id:
                ed.load(db_obj, trace_id)
                ed.mark_pre_analyzing(db_obj)
            else:
                there_are_more = ed.load_pending(db_obj)
            if there_are_more:
                print(("Analyzing experiment {0}".format(ed._trace_id)))
                er = AnalysisRunnerSingle(ed)
                er.do_full_analysis(db_obj)
            if trace_id:
                break
    def do_work_second_pass(self, db_obj, pre_trace_id):
        """Takes three experiments, and repeast the workflow analysis for
        each experiment but only taking into account the first n workflows
        in each one. n = minimum number of workflows acroos the three traces.
        Args:
        - db_obj: DB object configured to access the analysis database.
        - trace_id: If set to an integer, it will analyze the
            experiments identified by trace_id, trace_id+1, trace_id+2.
        """
        there_are_more=True
        while there_are_more:
            ed_manifest = ExperimentDefinition()
            ed_single = ExperimentDefinition()
            ed_multi = ExperimentDefinition()
            if pre_trace_id:
                trace_id=int(pre_trace_id)
                ed_manifest.load(db_obj, trace_id)
                there_are_more=True
            else:
                there_are_more = ed_manifest.load_next_ready_for_pass(db_obj)
                trace_id=int(ed_manifest._trace_id)
            if there_are_more:
                ed_single.load(db_obj, trace_id+1)
                ed_multi.load(db_obj, trace_id+2)
                ed_list=[ed_manifest, ed_single, ed_multi]
                print(("Reading workflow info for traces: {0}".format(
                    [ed._trace_id for ed in ed_list])))
                if (ed_manifest._workflow_handling!="manifest" or
                    ed_single._workflow_handling!="single" or
                    ed_multi._workflow_handling!="multi"):
                    print(("Incorrect workflow handling for traces"
                           "({0}, {1}, {2}): ({3}, {4}, {5})",format(
                               ed_manifest._trace_id,
                               ed_single._trace_id,
                               ed_multi._trace_id,
                               ed_manifest._workflow_handling,
                               ed_single._workflow_handling,
                               ed_multi._workflow_handling)
                           ))
                    print ("Exiting...")
                    exit()

                for ed in ed_list:  
                    ed.mark_pre_second_pass(db_obj)
                num_workflows=None
                for ed in ed_list:
                    exp_wfs=self.get_num_workflows(db_obj, ed._trace_id)
                    if num_workflows is None:
                        num_workflows = exp_wfs
                    else:
                        num_workflows=min(num_workflows, exp_wfs)
                print(("Final workflow count: {0}".format(num_workflows)))
                for ed in ed_list:
                    print(("Doing second pass for trace: {0}".format(
                        ed._trace_id)))
                    er = AnalysisRunnerSingle(ed)
                    er.do_workflow_limited_analysis(db_obj, num_workflows)
                print(("Second pass completed for {0}".format(
                    [ed._trace_id for ed in ed_list])))
            if pre_trace_id:
                break
    def get_num_workflows(self, db_obj, trace_id):
        result_type="wf_turnaround"
        key=result_type+"_stats"
        result = NumericStats()
        result.load(db_obj, trace_id, key)
        return int(result._get("count"))
            
            
        
        
      
    def do_work_delta(self, db_obj, trace_id=None, sleep_time=60):
        """Processes delta type experiment results.
        Args:
        - db_obj: DB object configured to access the analysis database.
        - sleep_time: wait time in seconds to wait between processing two delta
            experiments.
        """
        there_are_more=True
        while there_are_more:
            ed = DeltaExperimentDefinition()
            if trace_id:
                ed.load(db_obj, trace_id)
                ed.mark_pre_analyzing(db_obj)
            else:
                there_are_more = ed.load_pending(db_obj)
            if there_are_more:
                if ed.is_it_ready_to_process(db_obj):
                    er = AnalysisRunnerDelta(ed)
                    er.do_full_analysis(db_obj)
                sleep(sleep_time)
            if trace_id:
                break
            
    def do_work_grouped(self, db_obj,  trace_id=None, sleep_time=60):
        """Processes grouped type experiment results.
        Args:
        - db_obj: DB object configured to access the analysis database.
        - sleep_time: wait time in seconds to wait between processing two
            grouped experiments.
        """
        there_are_more=True
        while there_are_more:
            ed = GroupExperimentDefinition()
            if trace_id:
                ed.load(db_obj, trace_id)
                ed.mark_pre_analyzing(db_obj)
            else:
                there_are_more = ed.load_pending(db_obj)
            if there_are_more:
                if ed.is_it_ready_to_process(db_obj):
                    print(("Analyzing grouped experiment {0}".format(
                                                                ed._trace_id)))
                    er = AnalysisGroupRunner(ed)
                    er.do_full_analysis(db_obj)
            if trace_id:
                break
            elif there_are_more:
                print(("There are grouped experiments to be processed, but,"
                    "their subtrace are not ready yet. Sleeping for {0}s."
                    "".format(sleep_time)))
            #    sleep(sleep_time)
            else:
                print("No more experiments to process, exiting.")
    def do_mean_utilizatin(self, db_obj, trace_id=None):
        ed = GroupExperimentDefinition()
        if trace_id:
            trace_id_list=[trace_id]
        else:
            trace_id_list=ed.get_exps_in_state(db_obj, "analysis_done")
            trace_id_list+=ed.get_exps_in_state(db_obj, "second_pass_done")
        print(("processing following group traces (utilization mean):{0}".format(
               trace_id_list)))
        for trace_id in trace_id_list:
            print(("Calculating for", trace_id))
            ed = GroupExperimentDefinition()
            ed.load(db_obj, trace_id=trace_id)
            er = AnalysisGroupRunner(ed)
            er.do_only_mean(db_obj)
            
                
                
    def do_work_grouped_second_pass(self, db_obj, pre_trace_id):
        """Takes three experiments, and repeast the workflow analysis for
        each experiment but only taking into account the first n workflows
        in each one. n = minimum number of workflows acroos the three traces.
        Args:
        - db_obj: DB object configured to access the analysis database.
        - trace_id: If set to an integer, it will analyze the
            experiments identified by trace_id, trace_id+1, trace_id+2.
        """
        there_are_more=True
        while there_are_more:
            ed_manifest = GroupExperimentDefinition()
            ed_single = GroupExperimentDefinition()
            ed_multi = GroupExperimentDefinition()
            if pre_trace_id:
                trace_id=int(pre_trace_id)
                ed_manifest.load(db_obj, trace_id)
                there_are_more=True
            else:
                there_are_more = ed_manifest.load_next_ready_for_pass(db_obj)
                trace_id=int(ed_manifest._trace_id)
            if there_are_more:
                ed_single.load(db_obj, trace_id+1)
                ed_multi.load(db_obj, trace_id+2)
                ed_list=[ed_manifest, ed_single, ed_multi]
                print(("Reading workflow info for traces: {0}".format(
                    [ed._trace_id for ed in ed_list])))
                if (ed_manifest._workflow_handling!="manifest" or
                    ed_single._workflow_handling!="single" or
                    ed_multi._workflow_handling!="multi"):
                    print(("Incorrect workflow handling for traces"
                           "({0}, {1}, {2}): ({3}, {4}, {5})",format(
                               ed_manifest._trace_id,
                               ed_single._trace_id,
                               ed_multi._trace_id,
                               ed_manifest._workflow_handling,
                               ed_single._workflow_handling,
                               ed_multi._workflow_handling)
                           ))
                    print ("Exiting...")
                    exit()

                for ed in ed_list:  
                    ed.mark_pre_second_pass(db_obj)

                list_num_workflows=[]
                for (st_1, st_2, st_3) in zip(ed_manifest._subtraces,
                                              ed_single._subtraces,
                                              ed_multi._subtraces):
                    num_workflows=None
                    for ed_id in [st_1, st_2, st_3]:
                        exp_wfs=self.get_num_workflows(db_obj, ed_id)
                        if num_workflows is None:
                            num_workflows = exp_wfs
                        else:
                            num_workflows=min(num_workflows, exp_wfs)
                    list_num_workflows.append(num_workflows)
                
                print(("Final workflow count: {0}".format(list_num_workflows)))
                for ed in ed_list:
                    print(("Doing second pass for trace: {0}".format(
                        ed._trace_id)))
                    er = AnalysisGroupRunner(ed)
                    er.do_workflow_limited_analysis(db_obj, list_num_workflows)
                print(("Second pass completed for {0}".format(
                    [ed._trace_id for ed in ed_list])))
            if pre_trace_id:
                break
        
        
    