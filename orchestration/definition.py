from machines import Edison2015,Edison
from datetime import datetime
from generate import TimeController
from stats.trace import ResultTrace
from stats import Histogram, NumericStats

class ExperimentDefinition(object):
    """
    This class contains the definition and state of a single scheduling
    experiment. The definition is used to generate the workload and configure
    the scheduler. The state keeps track of the experiment: if it has been run,
    its output analyzed, etc. It allows to load and store data from/on a
    database.
    """
    
    def __init__(self,
                 name=None,
                 experiment_set=None,
                 seed="AAAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=None,
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_share=0.0,
                 workflow_handling="manifest",
                 subtraces = None,
                 start_date = datetime(2015,1,1),
                 preload_time_s = 3600*24*2,
                 workload_duration_s = 3600*24*7,
                 work_state = "fresh",
                 analysis_state = "0",
                 overload_target=0.0,
                 table_name="experiment",
                 conf_file=""):
        """Constructor allows to fully configure an experiment. However, a
        Definition can be created with default values and then loaded from
        the database.
        Args:
        - name: String with a human readable description of the experiment. If
            not set, name is set to value derived from the rest of the
            arguments.
        - experiment_set: String that identifies the group of experiments to
            which this one belongs.  If not set, it is constructed from the 
            experiment parameters.
        - machine: string identifying the system that the scheduling simulation
            must model in terms of hardware, and prioirity policies.
        - trace_type: string with three values "single", "delta", "group". A
            single experiment is a run of a workload that is analyzed. A delta
            is the comparison of the workflows in two single traces (listed in
            subtraces), and a group experiment aggregates the results of a
            number of single experiments (listed in subtraces).
        - manifest_list: List of dictionaries. Each dict has two keys: 
            "manifest", which value lists the name of the manifest file for a
            workflow type; and "share" a 0-1 value indicating the chance for a
            workflow to be of "manifest" type in the workload.
        - workflow_policy: string that governs how workflows are calculated to
            be added in the workload. Can take three values "no", no workflows;
            "period", one workflow every workflow_period_s seconds; and "share",
            workflow_share % of the jobs will be workflows.
        - workflow_period: positive number indicating how many seconds will be
            between two workflows in the workload.
        - workflow_share: float between 0 and 100 representing the % of jobs
            that are workflows.
        - workflow_handling: string governing how workflows are run and
            scheduled in the experiment. It can take three values: "single",
            where workflows are submitted as a single job; "multi", where
            each task in a workflow is run in an independent job; and "manifest"
            where workflows are submitted as a single job, but workflow aware
            backfilling is used.
        - subtraces: list of trace_id (int) of traces that should be used in the
            analysis of this experiment. Only valid for delta and group
            experiments.
        - start_date: datetime object pointing at the beginning of the
            generated workload.
        - pre_load_time_s: number of seconds of workload to be generated before
            the start_date. This workload is used to "load" the scheduler, but
            the analysis will be performed only from the "start_date".
        - workload_duration_s: number of seconds of workload to be generated
            after the start_date.
        - work_state: string representing the state of the experiment.
            possible values: "fresh", "pre_simulating", "simulating", 
            "simulation_done", "simulation_error", "pre_analyzing",
            "analyzing", "analysis_done", "analysis_error".
        - analysis_state: sub step of the analysis phase.
        - overload_target: if set to > 1.0, the workload generated will produce
            extra jobs during preload so in a period of time, overload_target
            times the capacity of the system (produced in that period) will be
            submitted.
        - table_name: dabatase table to store and load the content of
            an experiment from.
        - conf_file: if set to sting, the experiment will be run using a
            configuration file of such name. Other settins will be overriden.        
        """
        if subtraces is None:
            subtraces = []
        if manifest_list is None:
            manifest_list = []
        self._name=name
        self._experiment_set=experiment_set
        self._seed=seed
        self._machine=machine
        self._trace_type=trace_type
        self._manifest_list=manifest_list
        self._workflow_policy=workflow_policy
        self._workflow_period_s=workflow_period_s
        self._workflow_share=workflow_share
        self._workflow_handling=workflow_handling
        self._subtraces = subtraces
        self._start_date = start_date
        self._preload_time_s = preload_time_s
        self._workload_duration_s = workload_duration_s
        self._work_state = work_state
        self._analysis_state = analysis_state
        self._overload_target = overload_target
        self._table_name = table_name
        self._conf_file = conf_file
        
        self._simulating_start=None
        self._simulating_end=None
        self._worker=""
        
        for man in [x["manifest"] for x in manifest_list]:
            if "_" in man or "_" in man:
                raise ValueError("A manifest name cannot contain the characters"
                                 " '_' or '-', found: {0}".format(man))
            

        self._trace_id = None
        self._owner = None
        self._ownership_stamp = None
        
        if self._experiment_set is None:
            self._experiment_set  = self._get_default_experiment_set()
        if self._name is None:
            self._name = "{0}-s[{1}]".format(self._experiment_set, self._seed)
    def get_true_workflow_handling(self):
        """Returns "no" if there are no workflows in the trace, the configured
        value on worfklow_poliy otherwise."""
        if self._workflow_policy=="no":
            return "no"
        else:
            return self._workflow_handling
    def get_machine(self):
        """
        Returns a Machine object corresponding to the machine configured.
        """
        if self._machine == "edison":
            return Edison2015()
        elif self._machine == "default":
            return Edison()
        raise ValueError("Unknown machine set: {}".format(self._machine))
    
    def get_overload_factor(self):
        return self._overload_target%1000;
    
    def get_forced_initial_wait(self):
        if self._overload_target>999:
            runtime=self._overload_target/1000;
            return runtime
        return 0
    
    def get_system_user_list(self):
        return ["tester:1000",
                "root:0"
                "linpack:300",
                "nobody:99",
                "dbus:81",
                "rpc:32",
                "nscd:28",
                "vcsa:69",
                "abrt:499",
                "saslauth:498",
                "postfix:89",
                "apache:48",
                "rpcuser:29",
                "nfsnobody:65534",
                "ricci:140",
                "haldaemon:68",
                "nslcd:65",
                "ntp:38",
                "piranha:60",
                "sshd:74",
                "luci:141",
                "tcpdump:72",
                "oprofile:16",
                "postgres:26",
                "usbmuxd:113",
                "avahi:70",
                "avahi-autoipd:170",
                "rtkit:497",
                "pulse:496",
                "gdm:42",
                "named:25",
                "snmptt:495",
                "hacluster:494",
                "munge:493",
                "mysql:27",
                "bsmuser:400",
                "puppet:52",
                "nagios:401",
                "slurm:106"
                ]
    def get_user_list(self):
        """
        Returns a list of strings with the usernames to be emulated.
        """
        return ["user1"]
    def get_qos_list(self):
        """
        Returns a list of the qos policies to be used in the workload.
        """
        return ["qos1"]
    def get_partition_list(self):
        """
        Returns a list of the partitions to be used in the workload.
        """
        return ["main"]
    def get_account_list(self):
        """
        Returns a list of accounts ot be used in the workload.
        """
        return ["account1"] 
    
    def get_trace_file_name(self):
        """
        Returns a file system safe name based on the experiment name for its
        workload file.
        """
        return self.clean_file_name(self._name+".trace")
    def get_qos_file_name(self):
        """
        Returns a file system safe name based on the experiment name for its
        qos file.
        """
        return self.clean_file_name(self._name+".qos")
    def get_users_file_name(self):
        """
        Returns a file system safe name based on the experiment name for its
        users file.
        """
        return self.clean_file_name(self._name+".users")
    def get_start_epoch(self):
        """
        Returns the start date of the experiment in epoch format (int).
        """
        return TimeController.get_epoch(self._start_date)
    
    def get_end_epoch(self):
        """
        Returns the ending date of the experiment in epoch format (int).
        """
        return (TimeController.get_epoch(self._start_date) + 
                self._workload_duration_s)
    
    def clean_file_name(self, file_name):
        """Returns a string with a file-system name verions of file_name."""
        return "".join([c for c in file_name if c.isalpha() 
                                             or c.isdigit()
                                             or c=='.'
                                             or c=="-"]).rstrip()
        
    def _manifest_list_to_text(self, manifest_list):
        """Serializes the manifest list into a string"""
        list_of_text=[]
        for one_man in manifest_list:
            list_of_text.append("{0}|{1}".format(
                                             one_man["share"],
                                             one_man["manifest"]))
        
        return ",".join(list_of_text) 
    def _text_to_manifest_list(self, manifest_text):
        """Deserializes a string into a manifest list"""
        manifest_list = []
        for man in manifest_text.split(","):
            if man == "":
                continue
            man_parts  = man.split("|")
            man_share = float(man_parts[0])
            man_file = man_parts[1]
            manifest_list.append({"share":man_share, "manifest":man_file})
        return manifest_list 
    
    def _get_default_experiment_set(self):  
        """Returns the default experiment set based on the experiment
        configuration."""
        conf_file_str=""
        if self._conf_file:
            conf_file_str="-"+self._conf_file
        return ("{0}-{1}-m[{2}]-{3}-p{4}-%{5}-{6}-t[{7}]-{8}d-{9}d-O{10}{11}"
            "".format(
            self._machine,
            self._trace_type,
            self._manifest_list_to_text(self._manifest_list),
            self._workflow_policy,
            self._workflow_period_s, 
            self._workflow_share, 
            self._workflow_handling,
            ",".join([str(t) for t in self._subtraces]),
            int(self._preload_time_s/(3600*24)),
            int(self._workload_duration_s/(3600*24)),
            self._overload_target,
            conf_file_str))
    
    def store(self, db_obj):
        """Stores the object into a database at the table self._table_name.
        Args:
        - db_obj: configured DBManager object that will store the data.
        Returns trace_id
        """
        keys= ["name",
                "experiment_set",
                "seed",
                "machine",
                "trace_type",
                "manifest_list",
                "workflow_policy",
                "workflow_period_s",
                "workflow_share",
                "workflow_handling",
                "subtraces", 
                "start_date",
                "preload_time_s", 
                "workload_duration_s", 
                "work_state", 
                "analysis_state",
                "overload_target",
                "conf_file"]
        values = [self._name,
                    self._experiment_set,
                    self._seed,
                    self._machine,
                    self._trace_type,
                    self._manifest_list_to_text(self._manifest_list),
                    self._workflow_policy,
                    self._workflow_period_s,
                    self._workflow_share,
                    self._workflow_handling,
                    ",".join([str(t) for t in self._subtraces]),
                    db_obj.date_to_mysql(self._start_date),
                    self._preload_time_s, 
                    self._workload_duration_s, 
                    self._work_state,
                    self._analysis_state,
                    self._overload_target,
                    self._conf_file]
        
        ok, insert_id = db_obj.insertValues(self._table_name, keys, values,
                                        get_insert_id=True)
        if not ok:
            raise Exception("Error inserting experiment in database: {0}"
                            "".format(values))
        self._trace_id = insert_id
        return self._trace_id
    
    def mark_pre_simulating(self, db_obj):
        return self.upate_state(db_obj, "pre_simulating")
    
    def mark_simulating(self, db_obj, worker_host=None):
        if worker_host:
            self.update_worker(db_obj,worker_host)
        self.update_simulating_start(db_obj)
        return self.upate_state(db_obj, "simulating")
        
    def mark_simulation_done(self, db_obj):
        self.update_simulating_end(db_obj)
        return self.upate_state(db_obj, "simulation_done")
        
    def mark_simulation_failed(self, db_obj):
        self.update_simulating_end(db_obj)
        return self.upate_state(db_obj, "simulation_failed")
    
    def mark_pre_analyzing(self, db_obj):
        return self.upate_state(db_obj, "pre_analyzing")
    
    def mark_analysis_done(self, db_obj):
        return self.upate_state(db_obj, "analysis_done")

    def mark_second_pass(self, db_obj):
        return self.upate_state(db_obj, "second_pass_done")

    def mark_pre_second_pass(self, db_obj):
        return self.upate_state(db_obj, "pre_second_pass")
        
    def upate_state(self, db_obj, state):
        """
        Sets the state of the experiment. 
        """
        old_state=self._work_state
        self._work_state = state
        return db_obj.setFieldOnTable(self._table_name, "work_state", state,
                               "trace_id", str(self._trace_id), 
                               "and work_state='{0}'".format(old_state))
    def update_worker(self, db_obj, worker_host):
        self._worker=worker_host
        return db_obj.setFieldOnTable(self._table_name, "worker", worker_host,
                               "trace_id", str(self._trace_id))
        
    def update_simulating_start(self, db_obj):
        return db_obj.setFieldOnTable(self._table_name, "simulating_start",
                                      "now()",
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
    
    def update_simulating_end(self, db_obj):
        return db_obj.setFieldOnTable(self._table_name, "simulating_end",
                                      "now()",
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
    
    def reset_simulating_time(self, db_obj):
        db_obj.setFieldOnTable(self._table_name, "simulating_end",
                                      0,
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
        return db_obj.setFieldOnTable(self._table_name, "simulating_start",
                                      0,
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
        
        
    def load(self, db_obj, trace_id):
        """Configures the object according to a row in self._table_name
        identified by trace_id.
        Args:
        - db_obj: configured DBManager object that will load the data from
        - trace_id: integer identifying the experiment data to load.
        """
        
        self._trace_id = trace_id
        keys= ["name",
                "experiment_set",
                "seed",
                "machine",
                "trace_type",
                "manifest_list",
                "workflow_policy",
                "workflow_period_s",
                "workflow_share",
                "workflow_handling",
                "subtraces", 
                "start_date",
                "preload_time_s", 
                "workload_duration_s", 
                "work_state", 
                "analysis_state",
                "overload_target",
                "conf_file",
                "simulating_start",
                "simulating_end",
                "worker"]
        data_dic=db_obj.getValuesDicList(self._table_name, keys, condition=
                                        "trace_id={0}".format(
                                        self._trace_id))
        if data_dic == False:
            raise ValueError("Experiment not found!")
        for key in keys:
            setattr(self, "_"+key, data_dic[0][key])
        
        self._manifest_list=self._text_to_manifest_list(self._manifest_list)
        self._subtraces = [int(x) for x in self._subtraces.split(",") if x!=""] 
    
    def load_fresh(self, db_obj):
        """Configures the object with the data of the first experiment with
        state="fresh", ordered by trace_id. Then set the state to 
        "pre_simulating".
        
        Returns True if load was succesful, False if no experiments with state
            "fresh" are available.
        """
        return self.load_next_state(db_obj, "fresh", "pre_simulating")
    
    def load_pending(self, db_obj):
        """Configures the object with the data of the first experiment with
        state="simulation_done", ordered by trace_id. Then set the state to 
        "pre_analyzing".
        
        Returns True if load was succesful, False if no experiments with state
            "fresh" are available.
        """
        return self.load_next_state(db_obj, "simulation_done", "pre_analyzing")

    def load_ready_second_pass(self, db_obj):
        """Configures the object with the data of the first experiment with
        state="simulation_done", ordered by trace_id. Then set the state to 
        "pre_analyzing".
        
        Returns True if load was succesful, False if no experiments with state
            "fresh" are available.
        """
        return self.load_next_state(db_obj, "simulation_done", "pre_analyzing")
    def load_next_state(self,db_obj, state, new_state, check_pending=False,
                        subtraces_state=None):
        """Configures the object with the data of the first experiment with
        state=fresh, ordered by trace_id. Then stes the state go new_state.
        The operation is concurrent safe, two codes running load_next_state
        for the same state will never receive the data from the same experiment.
        Args:
        - db_obj: DBManager object configured to access a datbases.
        - state: state of the experiment to be searched for.
        - new_state: experiment to be set on both the db and the object once
            the experiment is loaded.
        - check_pending: checks if the the sub_traces are in a particular
            state.
        - subtraces_state: Expected state of the subtraces to performa load
            nad state set.
        Returns: False if no more experiments with
            state "state" are available, True otherwise.
        """
        update_ok=False
        data_left=True
        count = 1000
        while data_left and not update_ok:
            db_obj.start_transaction()
            rows=db_obj.getValuesAsColumns(self._table_name, ["trace_id"], 
                             condition = "work_state='{0}' "
                                         "and trace_type='{1}' ".format(
                                                           state, 
                                                           self._trace_type),
                             orderBy="trace_id")
            data_left = len(rows["trace_id"])>0
            if data_left:
                found_good=False
                for trace_id in rows["trace_id"]:
                    self.load(db_obj,int(trace_id))
                    found_good = (not check_pending 
                                  or self.are_sub_traces_analyzed(
                                                        db_obj,
                                                        subtraces_state))
                    if found_good:
                        break
                if not found_good:
                    db_obj.end_transaction()
                    break
                
                update_ok = self.upate_state(db_obj, new_state)
                    
            db_obj.end_transaction()
            if count == 0:
                raise Exception("Tried to load an experiment configuration many"
                            " times and failed!!")
            count -= 1
        return data_left
    def get_exps_in_state(self, db_obj, state):
        rows=db_obj.getValuesAsColumns(self._table_name, ["trace_id"], 
                             condition = "work_state='{0}' "
                                         "and trace_type='{1}' ".format(
                                                           state, 
                                                           self._trace_type),
                             orderBy="trace_id")
        return rows["trace_id"]
    def pass_other_second_pass_requirements(self, db_obj):
        return True
    
    def load_next_ready_for_pass(self,db_obj, state="analysis_done",
                                  new_state="pre_second_pass",
                        workflow_handling="manifest",
                        workflow_handling_list=["single", "multi"]):
       
        update_ok=False
        data_left=True
        count = 100
        """ Changes:
            - it passes over the ones that not good yet
            - does not use subtraces
        """
        while data_left and not update_ok:
            db_obj.start_transaction()
            rows=db_obj.getValuesAsColumns(self._table_name, ["trace_id"], 
                             condition = "work_state='{0}' "
                                 "and trace_type='{1}' "
                                 "and workflow_handling='{2}'".format(
                                                   state, 
                                                   self._trace_type,
                                                   workflow_handling),
                             orderBy="trace_id")
            data_left = len(rows["trace_id"])>0
            this_is_the_one=False
            if data_left:
                for trace_id in rows["trace_id"]:
                    self.load(db_obj,int(trace_id))
                    other_defs_ok=True
                    for (other_handling, t_id) in zip(
                            workflow_handling_list,
                            [trace_id+x+1 for x in range(
                                len(workflow_handling_list))]):
                        new_def=self.get_exp_def_like_me()
                        new_def.load(db_obj, t_id)
                        other_defs_ok=(other_defs_ok and 
                            new_def._work_state=="analysis_done" and
                             new_def._workflow_handling==other_handling and
                             new_def.pass_other_second_pass_requirements(db_obj))
                    if (not other_defs_ok or
                        not self.pass_other_second_pass_requirements(db_obj)):
                        continue
                    else:
                        this_is_the_one=True
                        break
                if this_is_the_one:
                    update_ok = self.upate_state(db_obj, new_state)
            db_obj.end_transaction()
            if count == 0:
                raise ValueError("Tried to load an experiment configuration many"
                            " times and failed!!")
            count -= 1
        return data_left
    def get_exp_def_like_me(self):
        return ExperimentDefinition()
    def del_results(self, db_obj):
        """Deletes all analysis results associated with this experiment"""
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(Histogram()._table_name, field, value)
        db_obj.delete_rows(ResultTrace()._get_utilization_result()._table_name,
                            field, value)
        db_obj.delete_rows(NumericStats()._table_name, field, value)
    
    def del_results_like(self, db_obj, like_field="type", like_value="lim_%"):
        """Deletes all analysis results associated with this experiment"""
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(Histogram()._table_name, field, value,
                           like_field, like_value)
        db_obj.delete_rows(ResultTrace()._get_utilization_result()._table_name,
                            field, value, like_field, like_value)
        db_obj.delete_rows(NumericStats()._table_name, field, value,
                           like_field, like_value)
        
    def del_trace(self, db_obj):
        """Deletes simulation trace associated with this experiment"""
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(ResultTrace()._table_name,
                            field, value)
    
    def del_exp(self, db_obj):
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(self._table_name,
                            field, value)
        
    
    
    def are_sub_traces_analyzed(self, db_obj, state):
        if not type(state) is list:
            state=[state]
        for trace_id in self._subtraces:
            rows=db_obj.getValuesAsColumns(self._table_name, ["work_state"], 
                             condition = "trace_id={0} ".format(trace_id))
            if len(rows["work_state"])==0:
                raise ValueError("Subtrace not found!")
            if not rows["work_state"][0] in state:
                return False
        return True
                                           
    
    def create_table(self, db_obj):
        """Creates a table valid to store Definition objects"""
        print ("Experiment table creation will fail if MYSQL Database does not"
               " support 'zero' values in timestamp fields. To zero values"
               " can be allowed by removing STRICT_TRANS_TABLES from 'sql_mode='"
               " in my.cnf."
               "")
        query = """
        create table `experiment` (
            `trace_id` int(10)  NOT NULL AUTO_INCREMENT,
            `name` varchar(512),
            `experiment_set` varchar(512),
            `seed` varchar(256),        # Alphanum seed for workload gen.
            `trace_type` varchar(64),   # single, delta, group
            `machine` varchar(64),      # Machine to simulate, e.g. 'edison'
            `manifest_list` varchar (1024), # Manifests to use in the trace. Format:
                                            # [{"manifest1.json":1.0}] or
                                            # [{"manifest1.json":0.5},{"manifest1.json":0.5}]
            `workflow_policy` varchar(1024),          # workflow submission policy:
                                                    #    'no', 'period', 'percentage'
            `workflow_period_s` INT DEFAULT 0,         # used in "period" policy.
                                                    #   seconds between two worflows.
            `workflow_share` DOUBLE DEFAULT 0.0,    # used in "percentage" policy
                                                    #    0-100% share of workflows over
                                                    #    jobs
            `workflow_handling` varchar(64),    # How workflows are submitted and
                                                #      scheduled: 'single', 'multi',
                                                #    'backfill'
            `start_date` datetime,       # epoch date where the trace should start
            `preload_time_s` int,         # lenght (in s.) to create filling workload at the
                                        # begining. It won't be analyzed.        
            `workload_duration_s` int,  # lenght (in s.) of the workload to be generated
            `subtraces` varchar(100),   # For the group and delta traces, what traces
                                        #     where used to build this one.
            `work_state` varchar(64),   # State of the simulation, analysis steps:
                                        #     'fresh', 'simulating', 'simulation done',
                                        #    'analyzing', 'analysis done'
            `analysis_state` varchar(64) DEFAULT "",  # States inside of the simulation. depending on
                                            #     trace_type and workflow_policy
            `owner` varchar(64) DEFAULT "",            # IP of the host that did the last update
            `conf_file` varchar(64) DEFAULT "", # Name of config file to be used in experiment
            `ownership_stamp` datetime,        # Time since last ownership.
            `overload_target` DOUBLE DEFAULT 1.1,   # Target cores-hours to be submitted
            `simulating_start` timestamp DEFAULT 0,
            `simulating_end` timestamp DEFAULT 0,
            `worker` varchar(256) DEFAULT "",
            PRIMARY KEY(`trace_id`)
            ) ENGINE = InnoDB;
        """
        db_obj.doUpdate(query)
    def is_it_ready_to_process(self):
        return self._work_state in ["analysis_done"]
    
    def is_analysis_done(self, second_pass=False):
        if second_pass:
            return self._work_state =="second_pass_done"
        return (self._work_state =="analysis_done" or
                self._work_state =="second_pass_done")
    
                                    
    
        
class GroupExperimentDefinition(ExperimentDefinition):
    """Grouped experiment definition: Experiment composed by multiple single
    experiments with the same scheduler and workload characteristics, but 
    different randome seed. Stats on worflow and job variables are calculated
    putting all traces togehter. Median is calculated over the utilizations.
    """
    def __init__(self,
                 name=None,
                 experiment_set=None,
                 seed="AAAAAA",
                 machine="edison",
                 trace_type="group",
                 manifest_list=None,
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_share=0.0,
                 workflow_handling="manifest",
                 subtraces = None,
                 start_date = datetime(2015,1,1),
                 preload_time_s = 3600*24*2,
                 workload_duration_s = 3600*24*7,
                 work_state = "pending",
                 analysis_state = "0", 
                 overload_target=0.0,
                 table_name="experiment"):
        super(GroupExperimentDefinition,self).__init__(
                                     name=name,
                                     experiment_set=experiment_set,
                                     seed=seed,
                                     machine=machine,
                                     trace_type=trace_type,
                                     manifest_list=manifest_list,
                                     workflow_policy=workflow_policy,
                                     workflow_period_s=workflow_period_s,
                                     workflow_share=workflow_share,
                                     workflow_handling=workflow_handling,
                                     subtraces = subtraces,
                                     start_date = start_date,
                                     preload_time_s = preload_time_s,
                                     workload_duration_s = workload_duration_s,
                                     work_state = work_state,
                                     analysis_state = analysis_state,
                                     overload_target=overload_target, 
                                     table_name=table_name)
    def load_pending(self, db_obj):
        """Configures the object with the data of the first experiment with
        state="fresh", ordered by trace_id. Then set the state to 
        "pre_simulating".
        
        Returns True if load was succesful, False if no experiments with state
            "fresh" are available.
        """
        return self.load_next_state(db_obj, "pending", "pre_analyzing", 
                                    True, ["analysis_done", "second_pass_done"])
    
    def add_sub_trace(self, trace_id):
        self._subtraces.append(trace_id)
    
    def is_it_ready_to_process(self, db_obj):
        """Returns true is the sub traces have been generated and analyzed."""
        for trace_id in self._subtraces:
            rt = ExperimentDefinition()
            rt.load(db_obj, trace_id)
            if not (rt._work_state in ["analysis_done", "second_pass_done"]):
                return False
        return True
    def pass_other_second_pass_requirements(self, db_obj):
        for sub_trace_id in self._subtraces:
            ex = ExperimentDefinition()
            ex.load(db_obj, sub_trace_id)
            if not ex.is_analysis_done():
                return False
        return True
    
    def get_exp_def_like_me(self):
        return GroupExperimentDefinition()


class DeltaExperimentDefinition(GroupExperimentDefinition):
    """Delta Experiments: Comparison between two single experiments with the
    same random seed, workload configuraion, but different scheduler
    configuration. Workflow variables are compared workflow to workflow, and
    statistics calculated over the differences.
    """
    def __init__(self,
                 name=None,
                 experiment_set=None,
                 seed="AAAAAA",
                 machine="edison",
                 trace_type="delta",
                 manifest_list=None,
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_share=0.0,
                 workflow_handling="manifest",
                 subtraces = None,
                 start_date = datetime(2015,1,1),
                 preload_time_s = 3600*24*2,
                 workload_duration_s = 3600*24*7,
                 work_state = "pending",
                 analysis_state = "0", 
                 table_name="experiment",
                 overload_target=None):
        super(GroupExperimentDefinition,self).__init__(
                                     name=name,
                                     experiment_set=experiment_set,
                                     seed=seed,
                                     machine=machine,
                                     trace_type=trace_type,
                                     manifest_list=manifest_list,
                                     workflow_policy=workflow_policy,
                                     workflow_period_s=workflow_period_s,
                                     workflow_share=workflow_share,
                                     workflow_handling=workflow_handling,
                                     subtraces = subtraces,
                                     start_date = start_date,
                                     preload_time_s = preload_time_s,
                                     workload_duration_s = workload_duration_s,
                                     work_state = work_state,
                                     analysis_state = analysis_state, 
                                     table_name=table_name,
                                     overload_target=overload_target)
    def add_compare_pair(self, first_id, second_id):
        self.add_sub_trace(first_id, second_id)
    
    
    def is_it_ready_to_process(self, db_obj):
        """Returns true is the sub traces have been at least generated."""
        for trace_id in self._subtraces:
            rt = ExperimentDefinition()
            rt.load(db_obj, trace_id)
            if not (rt._work_state in ["simulation_done", "analysis_done"]):
                return False
        return True
    
    def get_exp_def_like_me(self):
        return DeltaExperimentDefinition()

        