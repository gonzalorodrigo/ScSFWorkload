""" Library to produce traces for the slurm simulator. 

A simulation trace is composed of
- Job list: jobs to be submitted during the simulation.
- User list: list of pars user:userid representing users submitting jobs.
- QOS list: List of qos policies present in the trace.

The Job list is a  binary file with a list of job records. Each job record is
encoded as a struct with the following format:



Author Gonzalo P. Rodrigo (gprodrigoalvarez@lbl.gov). 
Copyright Lawrence Berkeley National Lab 2015.
 
"""

import struct
import os
import subprocess

class TraceGenerator(object):
    """Class to generate all the elements of a simulator trace. qos and user
    lists are generated from the detected users and qos in the submitted jobs.
    """
    
    def __init__(self):
        self._job_list = []
        self._user_list = []
        self._account_list = []
        self._qos_list = []
        self._submitted_core_s = 0
        self._first_submit_time = -1
        self._last_submit_time = -1
        self._decay_window_size = -1
        self._decay_window_stamps = None
        self._decay_window_values = None
        self._total_submitted_core_s = 0
        self._total_actual_core_s = 0
        self._total_actual_wf_core_s=0
        
    
    def add_job(self, job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest=None,
                  cores_s=None, ignore_work=False, real_core_s=None):
        """Ad a job with the observed characteristics. wclimit is in minutes"""
        self._job_list.append(get_job_trace(job_id=job_id, username=username, 
                                           submit_time=submit_time, 
                                           duration=duration, wclimit=wclimit,
                                           tasks=tasks,
                                           cpus_per_task=cpus_per_task,
                                           tasks_per_node=tasks_per_node, 
                                           qosname=qosname, partition=partition,
                                           account=account,
                                           reservation=reservation,
                                           dependency=dependency,
                                           workflow_manifest=workflow_manifest))
        
        if not username in self._user_list:
            self._user_list.append(username)
        
        if not account in self._account_list:
            self._account_list.append(account)
        
        if not qosname in self._qos_list:
            self._qos_list.append(qosname)
        
        if cores_s is None:
            cores_s= min(wclimit*60,duration) *tasks*cpus_per_task
        if not ignore_work:
            is_workflow=(workflow_manifest and (workflow_manifest[0]!="|" or 
                                      len(workflow_manifest)>1))
            self._add_work(submit_time, cores_s, real_work=real_core_s,
                           is_workflow=is_workflow)
            
    
    def _add_work(self, submit_time, work, real_work=None, is_workflow=False):
        """Records the core hours of a job.
        Args:
        - submit_time: epoch timestamp of the job submission.
        - work: core seconds requested by the job. 
        - real_work: if it is a workflow job containing jobs inside, it is
            the work of the bounding box. 
        """
        self._submitted_core_s+=work
        if real_work is not None:
            self._total_submitted_core_s+=real_work
        else:
            self._total_submitted_core_s+=work
        
        self._total_actual_core_s+=work
        if is_workflow:
            self._total_actual_wf_core_s+=work
        
        if (self._first_submit_time == -1):
            self._first_submit_time = submit_time
        self._last_submit_time=submit_time
        if self._decay_window_size>0:
            self._decay_window_stamps.append(submit_time)
            self._decay_window_values.append(work)
            while (self._decay_window_stamps and
                   self._decay_window_stamps[0] < 
                     (submit_time-self._decay_window_size)):
                self._submitted_core_s -= self._decay_window_values[0]
                self._decay_window_stamps = self._decay_window_stamps[1:]
                self._decay_window_values = self._decay_window_values[1:]
                self._first_submit_time = self._decay_window_stamps[0]
   
    def reset_work(self):
        self._first_submit_time=-1
        self._last_submit_time=-1
        self._submitted_core_s=0
        self._decay_window_stamps=[]
        self._decay_window_values=[]
        self._total_submitted_core_s=0
        self._total_actual_core_s=0
        self._total_actual_wf_core_s=0
          
    def get_share_wfs(self):
        if not self._total_actual_core_s:
            return None
        return (float(self._total_actual_wf_core_s)
                / float(self._total_actual_core_s))
    
    def set_submitted_cores_decay(self, decay_window_size):
        """Configures the size in seconds of the decay window for. 
        get_submitted_core_s. Must be used before the trace generation starts.
        Args:
        - decay_window_size: number of seconds to look into the past for the
            window. if set to <=0 decay is deactivated.
        """
        self._decay_window_size=decay_window_size
        self._decay_window_stamps = []
        self._decay_window_values = []
        
    def get_submitted_core_s(self):
        """Returns core-seconds submitted so far and difference between the
        submit of the first an the last submitted job. set_submited_cores_decay
        is set, only the jobs within the last configured seconds are
        taken into account.
        Returns:
        - int of core-seconds submitted so far.
        - int of the seconds between the submit time of the first and last job.
        """
        return  (self._submitted_core_s,
                 max(1,self._last_submit_time-self._first_submit_time))
    
    def get_total_submitted_core_s(self):
        return self._total_submitted_core_s
    
    def get_total_actual_cores_s(self):
        return self._total_actual_core_s;
        
    def dump_trace(self, file_name):
        """Dump job list."""
        f = open(file_name, 'w')
        for job in self._job_list:
            f.write(job)
        f.close()
    
    def dump_users(self, file_name, extra_users=[]):
        """Dump user list with the format user:userid per line."""
        start_count = 1024
        user_ids=range(start_count, start_count+len(self._user_list))
        f = open(file_name, 'w')
        for (user, userid) in zip(self._user_list,user_ids):
            if not (":" in user):
                user+=":"+str(userid)
            f.write(user+"\n")
        for user in extra_users:
            f.write(user+"\n")
        f.close()
    
    def dump_qos(self, file_name):
        """Dump QOS list, one name per line-"""
        f = open(file_name, 'w')
        for qos in self._qos_list:
            f.write(qos+'\n') 
        f.close()
    
    def free_mem(self):
        del self._job_list
        self._job_list=[]
    

def get_job_trace(job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest=None):
    """Returns the struct packing of the job data. Format:
        typedef struct job_trace {
            int job_id;
            char username[MAX_USERNAME_LEN];
            long int submit; /* relative or absolute? */
            int duration;
            int wclimit; (in minutes)
            int tasks;
            char qosname[MAX_QOSNAME];
            char partition[MAX_QOSNAME];
            char account[MAX_QOSNAME];
            int cpus_per_task;
            int tasks_per_node;
            char reservation[MAX_RSVNAME];
            char dependency[MAX_RSVNAME];
            struct job_trace *next;
        } job_trace_t;
        
        MAX_USERNAME_LEN, MAX_QOSNAME, MAX_RSVNAME = 30
""" 
    if workflow_manifest is None:
        buf=struct.pack("i30sliii30s30s30sii30s1024sP",job_id, username,
                        submit_time, 
                        duration, wclimit, tasks, qosname, partition, account, 
                        cpus_per_task, tasks_per_node, reservation, dependency,
                        0)
    else:
        buf=struct.pack("li30sliii30s30s30sii30s1024sP1024sP",0xFFFFFFFF,
                        job_id, username,
                        submit_time, 
                        duration, wclimit, tasks, qosname, partition, account, 
                        cpus_per_task, tasks_per_node, reservation, dependency,
                        0, workflow_manifest, 0)
    
   
    return buf


def extract_task_info(field):
    #23(2,1) 23 tasks, 2 tasks per node, 11 cpus per task

    num_tasks = int(field.split("(")[0])
    tasks_per_node =int(field.split("(")[1].split(")")[0].split(",")[0])
    cores_per_task =int(field.split("(")[1].split(")")[0].split(",")[1])
    return num_tasks, tasks_per_node, cores_per_task

def extract_records(file_name="test.trace", 
                    list_trace_location="./list_trace"):
    """Reads a binary list job file and returns a list of dict (one per job)
    with the job characteristics.
    
    Returns: A list of dict, each dict is a job. The dict keys are: JOBID,
        USERNAME, PARTITION, ACCOUNT, QOS, SUBMIT, DURATION, WCLIMIT, TASKS,
        RES, DEP, NUM_TASKS, TASKS_PER_NODE, CORES_PER_TASK
    """

    print [list_trace_location, '-w', file_name]
    proc = subprocess.Popen([list_trace_location, '-w', file_name], 
                            stdout=subprocess.PIPE)
    still_header=True
    col_names=None
    
    records_list=[]
    
    while True:
        line=proc.stdout.readline()
        if line is None or line=="":
            break
        if "JOBID" in line and col_names is None:
            col_names=line.split()
        elif not still_header:
            col_values=line.split()
            record=dict()
            for (key, value) in zip(col_names, col_values):
                record[key]=value
            if (len(col_values) > len(col_names)):
                extra_values=col_values[len(col_names):]
                for extra in extra_values:
                    words=extra.split("=")
                    if (words[0]=="DEP" or words[0]=="RES") and len(words)==2:
                        record[words[0]]=words[1]
            num_tasks, tasks_per_node, cores_per_task=extract_task_info(
                                                            record["TASKS"])
            record["NUM_TASKS"] = num_tasks
            record["TASKS_PER_NODE"] = tasks_per_node
            record["CORES_PER_TASK"] = cores_per_task            
            records_list.append(record)
        elif "====" in line:
            still_header=False
    return records_list  
