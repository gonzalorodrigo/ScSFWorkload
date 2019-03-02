from datetime import timedelta
from os import path
import os
import shutil
import subprocess
from time import sleep

from generate import WorkloadGenerator, TimeController
from generate.overload import OverloadTimeController
from generate.pattern import (WorkflowGeneratorSingleJob, RepeatingAlarmTimer,
                              WorkflowGeneratorMultijobs, PatternGenerator)
from generate.special import SpecialGenerators
from generate.special.machine_filler import filler
import random_control
from slurm.trace_gen import TraceGenerator
from stats.trace import ResultTrace
from tools.ssh import SSH


class ExperimentRunner(object):
    """Class capable of taking an experiment definition, generate its workload,
    run the experiment, and import to store the results. 
    """
        
    @classmethod   
    def configure(cld,
                  trace_folder="/TBD/",
                  trace_generation_folder="/TBD/",
                  local=True, run_hostname=None,
                  run_user=None,
                  local_conf_dir="/TBD",
                  scheduler_conf_dir="/TBD/",
                  scheduler_conf_file_base="slurm.conf",
                  scheduler_folder="/TBD",
                  scheduler_script="run_sim.sh",
                  stop_sim_script = "stop_sim.sh",
                  manifest_folder="manifests",
                  scheduler_acc_table="perfdevel_job_table",
                  drain_time=6*3600):
        """ This class method configures runtime parameters needed by
        all ExperimentRunner instances.
        Args:
        - trace_folder: final file system destination (local or remote) where
            the workload file will be stored.
        - trace_generation_folder: local file system folder where workload files
            can be temporarily generated.
        - local: If false, all the operations to copy files or execute commands
            will be perform in the host executing the code. If True, those
            operation will be performed over ssh against "run_hostname". Remote
            mode requires passwordless ssh access for run_user.
        - local_conf_dir: local file system location where ExperimentRunner
            will find configuration model files to configure the experiment
            scheduler.
        - scheduler_conf_dir: local or remote route to the experiment's
            scheduler configuration folder.
        - scheduler_conf_file_base: name of the experiment's
            scheduler configuration file.
        - scheduler_folder: local or remote folder for the experiment's
            scheduler scheduler_script and stop_sim_sript script files.
        - scheduler_script: name of the script starting the scheduler 
            simulation.
        - stop_sim_script: name of the script capable of stoping the scheduler
            simulation.
        - manifest_folder: local folder where the ExperimentRunner can find the
            manifests to be used in the simulation.
        - scheduler_acc_table: name of the table used in the scheduler's
            dabatase to store job accounting information.
        """
        cld._trace_folder= trace_folder
        cld._trace_generation_folder = trace_generation_folder
        cld._local = local
        cld._run_hostname = run_hostname
        cld._run_user = run_user
        cld._scheduler_conf_dir=scheduler_conf_dir
        cld._scheduler_conf_file_base=scheduler_conf_file_base
        cld._local_conf_dir=local_conf_dir
        cld._scheduler_folder=scheduler_folder
        cld._scheduler_script=scheduler_script
        cld._stop_sim_script=stop_sim_script
        cld._manifest_folder=manifest_folder
        cld._scheduler_acc_table = scheduler_acc_table
        cld._drain_time = drain_time
    
    @classmethod
    def get_manifest_folder(cld):
        if hasattr(cld, "_manifest_folder"):
            return cld._manifest_folder
        return "./"
        
    def __init__(self, definition):
        """Constructor.
        Args:
        - definition: Definition object containing the configuration of the
            experiment to be run by this object.
        """
        self._definition=definition
    
    
    def do_full_run(self, scheduler_db_obj, store_db_obj):
        """ Creates a workload trace and sets the scheduler configuration
        according to the self._definition. Then ployes them in the scheduler,
        runs the simulation, imports the resulting trace, and stores in a
        database. It also changes the state of the experiment accordingly.
        Args:
        - scheduler_db_obj: DBManager object configured to connect to the
            schedulers's database.
        - store_db_obj: DBManager object configured to connect to the database
            where results traces should be stored.
        Returns True if the simulation produced a valid trace. False otherwise.
        """
        self._refresh_machine()
        self.create_trace_file()
        self.do_simulation()
        self._definition.mark_simulating(store_db_obj,
                                     worker_host=ExperimentRunner._run_hostname)
        self.wait_for_sim_to_end()
        if self.check_trace_and_store(scheduler_db_obj, store_db_obj):
            self._definition.mark_simulation_done(store_db_obj)
            self.clean_trace_file()
            return True
        else:
            self._definition.mark_simulation_failed(store_db_obj)
            return False

    
    def check_trace_and_store(self, scheduler_db_obj, store_db_obj):
        """ Imports the result trace from an experiment and stores it in a 
        central database.
        Args:
         - scheduler_db_obj: DBManager object configured to connect to the
            schedulers's database.
        - store_db_obj: DBManager object configured to connect to the database
            where results traces should be stored.
        Returns True if the simulation produced a valid trace. False otherwise.
        """
        result_trace = ResultTrace()
        result_trace.import_from_db(scheduler_db_obj, 
                                    ExperimentRunner._scheduler_acc_table)
        
        status=True
        end_time = self._definition.get_end_epoch()
        if len(result_trace._lists_start["time_end"])==0:
            print("Error: No simulated jobs")
            return False
        last_job_end_time=result_trace._lists_submit["time_submit"][-1]
        if last_job_end_time < (end_time-600):
            print(("Simulation ended too soon: {0} vs. expected {1}.".format(
                                                    last_job_end_time,
                                                    end_time)))
            status= False
        result_trace.store_trace(store_db_obj, self._definition._trace_id)    
        return status
        
    def create_trace_file(self):
        """Creates the workload files according the Experiment definition
        and stores them in the the scheduler. These files are composed by a job
        submission list and a list of valid users.
        """
        file_names = self._generate_trace_files(self._definition)
        self._place_trace_file(file_names[0])
        self._place_users_file(file_names[2])


    def _generate_trace_files(self, definition, trace_generator=None):
        """Creates the workload files according an Experiment definition.
        Args:
        - definition: Definition object defining the experiment.
        """
        if trace_generator is None:
            trace_generator = TraceGenerator()
        print(("This is the seed to be used:", definition._seed))
        random_control.set_global_random_gen(seed=definition._seed)
        machine=definition.get_machine()
        (filter_cores, filter_runtime, 
            filter_core_hours) = machine.get_filter_values()
        
        wg = WorkloadGenerator(machine=definition.get_machine(),
                          trace_generator=trace_generator,
                          user_list=definition.get_user_list(),
                          qos_list=definition.get_qos_list(),
                          partition_list=definition.get_partition_list(),
                          account_list = definition.get_account_list())       
        if definition._workflow_policy.split("-")[0] == "sp":
            special_gen = SpecialGenerators.get_generator(
                                definition._workflow_policy,
                                wg,
                                register_datetime=(definition._start_date - 
                                    timedelta(0,definition._preload_time_s)))
            wg.register_pattern_generator_timer(special_gen)
        else:
            wg.config_filter_func(machine.job_can_be_submitted)
            wg.set_max_interarrival(machine.get_max_interarrival())
            if definition._trace_type != "single":
                raise ValueError("Only 'single' experiments require trace "
                                 "generation")
            if definition.get_overload_factor()>0.0:
                print(("doing overload:", definition.get_overload_factor()))
                max_cores=machine.get_total_cores()
                single_job_gen = PatternGenerator(wg)
                overload_time = OverloadTimeController(
                                   single_job_gen,
                                   register_datetime=(definition._start_date - 
                                    timedelta(0,definition._preload_time_s)))
                overload_time.configure_overload(
                                trace_generator,
                                max_cores,
                            overload_target=definition.get_overload_factor())
                print(("about to register", wg, overload_time))    
                wg.register_pattern_generator_timer(overload_time)
          
            manifest_list = [m["manifest"] for m in definition._manifest_list]
            share_list = [m["share"] for m in definition._manifest_list]
            if (definition._workflow_handling == "single" or
                definition._workflow_handling == "manifest"):
                flow = WorkflowGeneratorSingleJob(manifest_list, share_list, wg)
            else:
                flow = WorkflowGeneratorMultijobs(manifest_list, share_list, wg)
            if definition._workflow_policy == "period":
                alarm = RepeatingAlarmTimer(flow,
                                    register_datetime=definition._start_date)
                alarm.set_alarm_period(definition._workflow_period_s)
                wg.register_pattern_generator_timer(alarm)
            elif definition._workflow_policy == "percentage":
                wg.register_pattern_generator_share(flow, 
                                                definition._workflow_share/100)
            
        target_wait=definition.get_forced_initial_wait()
        if target_wait:
            default_job_separation=10
            separation = int(os.getenv("FW_JOB_SEPARATION",
                                   default_job_separation))
            filler(wg,
                   start_time=TimeController.get_epoch((definition._start_date - 
                            timedelta(0,definition._preload_time_s))),
                   target_wait=target_wait,
                   max_cores=machine.get_total_cores(),
                   cores_per_node=machine._cores_per_node,
                   job_separation=separation)
            trace_generator.reset_work()

        wg.generate_trace((definition._start_date - 
                           timedelta(0,definition._preload_time_s)),
                          (definition._preload_time_s +
                            definition._workload_duration_s))
        max_cores=machine.get_total_cores()
        total_submitted_core_s=trace_generator.get_total_submitted_core_s()
        job_pressure=(float(total_submitted_core_s)
                      /
                      float((definition._preload_time_s +
                            definition._workload_duration_s)*max_cores)
                      )
        print(("Observed job pressure (bound): {0}".format(
                    job_pressure)))
                           
        trace_generator.dump_trace(path.join(
                                      ExperimentRunner._trace_generation_folder,
                                      definition.get_trace_file_name()))
        trace_generator.dump_qos(path.join(
                                      ExperimentRunner._trace_generation_folder,
                                      definition.get_qos_file_name()))
        trace_generator.dump_users(path.join(
                                      ExperimentRunner._trace_generation_folder,
                                      definition.get_users_file_name()),
                                   extra_users=definition.get_system_user_list()
                                   )
        trace_generator.free_mem()
        return [definition.get_trace_file_name(),
                definition.get_qos_file_name(), 
                definition.get_users_file_name()]
    def _place_trace_file(self, filename):
        """Places the workload files in the scheduler: job submission list and
        user list. Reads them from ExperimentRunner._local_trace_files 
        Args:
        - filename: string with the name of the workload files.
        """
        source = path.join(
                        ExperimentRunner._trace_generation_folder, filename)
        dest =  path.join(
                        ExperimentRunner._trace_folder, filename)
        self._copy_file(source, dest, move=True)
        
        for manifest in self._definition._manifest_list:
            man_name=manifest["manifest"]
            man_route_orig=path.join(ExperimentRunner._manifest_folder, 
                                     man_name)
            man_route_dest=path.join(ExperimentRunner._scheduler_folder, 
                                     man_name)
            self._copy_file(man_route_orig, man_route_dest)
    
    def _place_users_file(self, filename):
        """Places the users list in the scheduler configuration folder. It
        is read from the local trace generation folder.
        Args:
        - filename: string with the name of the users files. 

        """
        source = path.join(
                        ExperimentRunner._trace_generation_folder, filename)
        dest =  path.join(
                        ExperimentRunner._scheduler_conf_dir, "users.sim")
        self._copy_file(source, dest, move=True)
      

    def clean_trace_file(self):
        """Removes the trace file placed in the scheduler.
        """
        filenames = [self._definition.get_trace_file_name()]
        for filename in filenames:
            dest =  path.join(
                        ExperimentRunner._trace_folder, filename)
            self._del_file_dest(dest)
            
    def do_simulation(self):
        """Configures the scheduler according to the experiments workflow
        handling configuration. Runs the simulation. Trace has to be placed
        already. 
        """
        self._configure_slurm()
        self._run_simulation()
    
    def _refresh_machine(self):
        """Reboots the worker and waits until it is ready."""
        command=["sudo", "/sbin/shutdown", "-r", "now"] 
        print("About to reboot the machine, waiting 60s")
        self._exec_dest(command)
        sleep(60)
        while not "hola" in self._exec_dest(["/bin/echo", "hola"]):
            print("Machine is not ready yet, waiting 30s more...")
            sleep(30)
        print("Wait done, machine should be ready.")

    def stop_simulation(self):
        """Stops the scheduler and simulator. Non graceful stop."""
        self._exec_dest([path.join(ExperimentRunner._scheduler_folder,
                               ExperimentRunner._stop_sim_script)])
    
    def _run_simulation(self):
        trace_file=path.join(
                        ExperimentRunner._trace_folder,
                        self._definition.get_trace_file_name())
        script_route=path.join(ExperimentRunner._scheduler_folder,
                               ExperimentRunner._scheduler_script)
        
        command=[script_route, str(self._definition.get_end_epoch()+
                                   ExperimentRunner._drain_time),
                        path.join(
                        ExperimentRunner._trace_folder, trace_file), 
                        "./sim_mgr.log"]
        print(("About to run simulation: \n{0}".format(" ".join(command))))
        self._exec_dest(command, background=True)
        running=False
        for i in range(6*10):
            sleep(10)
            try:
                running = self.is_sim_running()
            except SystemError:
                print("Failed comms to machine, keep trying.")

            print(("Is Simulation running?", running))
            if running:
                break
        if not running:
            raise Exception("Error Starting simulation!!!!")
        
        
          
 
    def _configure_slurm(self):
        """Sets the configuration file for the scheduler, selecting a particular
        workflow scheduling policy."""
        cad="regular"
        if self._definition._workflow_handling=="manifest":
            cad="wfaware"
        orig_conf_file = "{2}.{0}.{1}".format(
                                    self._definition._machine,
                                    cad,
                                    ExperimentRunner._scheduler_conf_file_base)
        if self._definition._conf_file:
            orig_conf_file = self._definition._conf_file
        dest_conf_file=ExperimentRunner._scheduler_conf_file_base
        
        orig=path.join(ExperimentRunner._local_conf_dir, orig_conf_file)
        dest=path.join(ExperimentRunner._scheduler_conf_dir, dest_conf_file)
        self._copy_file(orig, dest)
    
    def _copy_file(self, orig, dest, move=False):
        if ExperimentRunner._local:
            shutil.copy(orig, dest)
        else:
            ssh = SSH(ExperimentRunner._run_hostname,
                          ExperimentRunner._run_user)
            ssh.push_file(orig, dest)
        if move:
            os.remove(orig)
    def _del_file_dest(self, dest):
        if ExperimentRunner._local:
                os.remove(dest)
        else:
            ssh = SSH(ExperimentRunner._run_hostname,
                      ExperimentRunner._run_user)
            ssh.delete_file(dest)
    def _exec_dest(self, command, background=False):
        if ExperimentRunner._local:
            if not background:
                p = subprocess.Popen(command, stdout=subprocess.PIPE)
                output, err = p.communicate()
                if err is None:
                    err=""
            else:
                output=""
                p = subprocess.Popen(command)
            
        else:
            ssh = SSH(ExperimentRunner._run_hostname,
                      ExperimentRunner._run_user)
            output, err, rc= ssh.execute_command(command[0], command[1:],
                                                 background=background)
        return output
    def is_simulation_done(self):
        """Returns True if the simulation engine is not running anymore"""
        #TODO(gonzalorodrigo): what if we check if the simulation is really
        # done? Now it only checks if the sim_mgr process is dead.
        return not self.is_sim_running()
    
    def wait_for_sim_to_end(self):
        """Blocks until the sim_mgr process stops running."""
        count = 0
        wait_time = 10
        failed_comms_count=0
        while True:
            try:
                if self.is_simulation_done():
                    break
                failed_comms_count=0
            except:
                failed_comms_count+=1
                print(("Failed commons while checking is sim was done, failed"
                      " count: {0}".format(failed_comms_count)))
            total_time=count*wait_time
            print(("Simulation has not ended. Wait time: {0}:{1}:{2}".format(
                                      total_time/3600, (total_time/60)%60,
                                      total_time%60  )))
            count+=1
            sleep(wait_time)
        
        
    
    def is_sim_running(self):
        """Returns True if all the process of the simulation are running."""
        try:
            if not self.is_it_running("sim_mgr"):
                return False
            if not self.is_it_running("slurmctld"):
                return False
            if not self.is_it_running("slurmd"):
                return False
            return True
        except SystemError:
            print("Error communicating to check remote processes")
            raise SystemError
            return True
    def is_it_running(self, proc):
        """Checks if a process named proc is running, locally or in the
        remote execution.
        Args:
        - proc: string with the name of the process to be checked.
        """
        output=self._exec_dest(["/bin/ps", "-eo comm,state"])
        count = 0
        total_count=0
        for line in output.split("\n"):
            total_count+=1
            if proc in line and not "Z" in line:
                count+=1
        if count>0:
            return True
        if total_count<5:
            raise SystemError()
        return False