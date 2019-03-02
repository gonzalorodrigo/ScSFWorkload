"""
This script will check the content of the experiment database looking for 
experiments that are not correct: the trace does not correspond to the
experiment definition.

This is possible when workers are not killed properly, still waiting for work
to be completed. Then, another worker might step in, take control of a VM
and start a new experiment. When this experiment is done, both workers extract
the trace, thinking that they both have the correct one. However, one of
them has the wrong one.

Args:

python clean_bad_traces.py [deep]

If "deep" is not specified, it will group all experiments with the same number
of jobs which trace_id>=180 and:
- check that their traces are valid: correct workflow type, scheduling policy.
- check that two traces that have the same seed don't have the same 1000 first
  jobs and 100 first workflow jobs.
  
If "deep" is speficied, it checks all experiments which trace_id>=180 and check
that there traces are valid.

After checking, the program asks from user confirmation to reset the state
of those experiments which are not valid. It also produces a list of "supicious
traces", i.e. pairs of traces with different seeds but similar jobs.

"""
"""
Get a list of experiments with the trace count.
SQL:
    select trace_id, count(*) cc from traces where trace_id>=180
        group by trace_id order by cc
"""

import sys

from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from stats.workflow import TaskTracker


db_obj = get_central_db()

do_deep=False

if len(sys.argv)>=2:
    do_deep=sys.argv[1]=="deep"
    

first_id = 180
if not do_deep:
    query = ("select trace_id, count(*) cc from traces where trace_id>={0} "
             "group by trace_id order by cc""".format(first_id))
else:
    query = ("select trace_id, count(*) cc from experiment where trace_id>={0} "
             "and (work_state='simulation_done' or work_state='analysis_done') "
             "and (trace_type='single') "
             "group by trace_id order by trace_id".format(first_id))

result=db_obj.doQueryDic(query)

""" Parse the llist and make a dic with of lists of trace_id's that have the
same number of jobs"""

exp_count={}

for res in result:
    try:
        traces_list = exp_count[res["cc"]]
    except KeyError as e:
        traces_list = []
        exp_count[res["cc"]] = traces_list
    traces_list.append(res["trace_id"])

""" Discard dic entries with one element. """

exp_count_multi={}
total_list_of_ids=[]
for key in exp_count:
    if len(exp_count[key])>1 or do_deep:
        exp_count_multi[key]=exp_count[key]
        total_list_of_ids+=exp_count[key]

del exp_count
print exp_count_multi

print ("Testing({0}): {1}".format(len(total_list_of_ids),
                                 " ".join([str(x) for x in total_list_of_ids])))

"""
Go in each entry in each list:one by one do they pass the test?
-Handling matches the workflow job names.
-the workflow corresponds to the job name
-the seed is different: Then it shoudl have different number of jobs!
"""
def get_workflow_jobs(db_obj, trace_id, number_of_jobs):
    query=("""select id_job, job_name, timelimit, cpus_req from traces"""
           """ where trace_id={0} and job_name!="sim_job" """
           """ and job_name!="allocation" limit {1};""".format(
               trace_id, number_of_jobs))
    return db_obj.doQueryDic(query)

def get_jobs(db_obj, trace_id, number_of_jobs):
    query=("""select id_job, job_name, timelimit, cpus_req from traces"""
           """ where trace_id={0} limit {1};""".format(
               trace_id, number_of_jobs))
    return db_obj.doQueryDic(query)

def check_trace_ok(db_obj, trace_id):
    exp = ExperimentDefinition()
    exp.load(db_obj, trace_id)
    
    workflow_names=[x["manifest"] for x in exp._manifest_list]
    
    job_list=get_workflow_jobs(db_obj, trace_id, 10)
    if len(job_list) == 0 and exp._workflow_policy!="no":
        print("Exp({0}) should have worklfow jobs (0 found)".format(trace_id))
        return False
    
    if exp._workflow_handling=="single":
        for job in job_list:
            name, stage_id, deps=TaskTracker.extract_wf_name(job["job_name"])
            if not name.split("-")[0] in workflow_names:
                print ("Exp({0}) uses a workflow that should not be there"
                       " job({1}): {2} vs {3}"
                       "".format(trace_id, job["id_job"], job["job_name"],
                                 workflow_names))
                return False
            if stage_id!="" or deps!=[]:
                print ("Exp({0}) is a single job workflows, and job({1}) is {2}"
                       "".format(trace_id, job["id_job"], job["job_name"]))
                return False
    elif exp._workflow_handling=="manifest":
        first_job=job_list[0]
        first_name, first_stage_id, first_deps=TaskTracker.extract_wf_name(
                        first_job["job_name"])
        if not first_name.split("-")[0] in workflow_names:
            print ("Exp({0}) uses a workflow that should not be there"
                       " job({1}): {2} vs {3}"
                       "".format(trace_id, first_job["id_job"],
                                 first_job["job_name"],
                                 workflow_names))
            return False
        if first_stage_id!="" or first_deps!=[]:
            print ("Exp({0}) uses manifests, the first job of the workflow"
                   " ({1} should not have dependencies: {2}"
                       "".format(trace_id, first_job["id_job"],
                                 first_job["job_name"]))
            return False
        other_tasks_found=False
        for job in job_list[1:]:
            name, stage_id, deps = TaskTracker.extract_wf_name(job["job_name"])
            if name==first_name and stage_id!="":
                other_tasks_found=True
                break
        if not other_tasks_found:
            print ("Exp({0}) uses manifests, however the tasks for the worklow "
                   "are not there: {1}".format(trace_id, first_job["job_name"]))
            return False
    elif exp._workflow_handling=="multi":
        for job in job_list:
            name, stage_id, deps = TaskTracker.extract_wf_name(job["job_name"])
            if not name.split("-")[0] in workflow_names:
                print ("Exp({0}) uses a workflow that should not be there"
                       " job({1}): {2} vs {3}"
                       "".format(trace_id, job["id_job"], job["job_name"],
                                 workflow_names))
                return False
            if stage_id=="":
                print ("Exp({0}) uses dependencies, however the tasks for "
                       "the worklow are not there: {1}".format(trace_id,
                                                        job["job_name"]))
                return False
    else:
        print ("Exp({0}) unknown worklfow handling: {1}".format(trace_id,
                                                        exp._workflow_handling))
        return False
    print ("Exp({0}) is correct".format(trace_id))
    return True

def compare_traces_jobs(db_obj, t1, t2, num_jobs, workflows=False):
    if not workflows:
        jobs1=get_jobs(db_obj, t1, num_jobs)
        jobs2=get_jobs(db_obj, t2, num_jobs)
    else:
        jobs1=get_workflow_jobs(db_obj, t1, num_jobs)
        jobs2=get_workflow_jobs(db_obj, t2, num_jobs)

    
    exp1=ExperimentDefinition()
    exp1.load(db_obj,t1)
    exp2=ExperimentDefinition()
    exp2.load(db_obj, t2)
    
    different=False
    for (job1,job2) in zip(jobs1,jobs2):
        if job1!=job2:
            different=True
            break
    if not different and exp1._seed!=exp2._seed:
        print ("Exps({0},{1}) have the exact same first {3} jobs with different"
               " seeds.".format(t1,t2,num_jobs))
        return False
    return True



def check_all_good(db_obj, good, num_jobs):
    suspicious_pairs = []
    for trace_id in good:
        new_good = list(good)
        new_good.remove(trace_id)
        for other_id in new_good:
            if not (compare_traces_jobs(db_obj, trace_id, other_id, num_jobs)):
                suspicious_pairs.append((trace_id, other_id))
            elif not (compare_traces_jobs(db_obj, trace_id, other_id,
                                          num_jobs/10,
                                          True)):
                suspicious_pairs.append((trace_id, other_id))
                
    return suspicious_pairs
        
            
    
all_good=[]
all_bad=[]
all_suspicious_pairs=[]
for key in exp_count_multi:
    good=[]
    bad=[]
    for trace_id in exp_count_multi[key]:
        if(check_trace_ok(db_obj, trace_id)):
            good.append(trace_id)
        else:
            bad.append(trace_id)
    if good>1 and not do_deep:
        all_suspicious_pairs+=check_all_good(db_obj, good, 200)
    all_good+=good
    all_bad+=bad

print ("""Final result:""")
print ("""Good traces({1}): {0}""".format(" ".join([str(x) for x in all_good]),
                                          len(all_good)))
print ("""Bad traces({1}): {0}""".format(" ".join([str(x) for x in all_bad]),
                                         len(all_bad)))
print ("""Suspicios pairs({1}): {0}""".format(" ".join([str(x) for x in
                                                   all_suspicious_pairs]),
                                              len(all_suspicious_pairs)))

theval=raw_input(
      "Press Enter to continue and reset those experiments that are not ok: "
      "{0}".format(" ".join([str(x) for x in all_bad])))

for bad_trace_id in all_bad:
    print ("Cleaning experiment trace and results: {0}".format(bad_trace_id))
    ed = ExperimentDefinition()
    ed.load(db_obj, bad_trace_id)
    ed.del_trace(db_obj)
    ed.del_results(db_obj)
    ed.upate_state(db_obj, "fresh")
    


