
import json
import sys

import pygraphviz as pgv
import xml.etree.ElementTree as ET


def convert_xml_wf_to_json_manifest(xml_route, json_route, grouped_jobs=[],
                                    max_cores=None, namespace=None):
    """ Reads a workflow definition from an xml file, transforms into a
    workflow aware backfiling json manifest and write it in another file.
    Args:
    - xml_route: file system route string pointing to an xml file containing 
        a workflow definition in the format used at: 
        https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
    - json_route: file system route string pointing to where a json file will
        be created containing the Manifest version of the read xml_route.
    - grouped_jobs: list of strings containing names of jobs present in the
        read xml. For each job name in grouped_jobs, all jobs with the such
        name will be grouped in a single job. 
    """
    print("Loading XML:", xml_route)
    xml_wf = ET.parse(xml_route)
    print("XML Loaded")
    if namespace is None:
        namespace=_get_namespace(xml_wf)
    print("Getting Jobs and dependencies")
    jobs, deps = _get_jobs_and_deps(xml_wf, namespace=namespace)
    print("Jobs and dependencies extraced: {0} jobs and {1} deps".format(
                                len(jobs), len(deps)))          
    del xml_wf    
    print("XML tree deallocated")
    print("Fusing jobs")
    jobs, job_fusing_dic= _fuse_jobs(jobs, grouped_jobs,
                                     max_cores=max_cores)
    print("Fusing jobs Done", job_fusing_dic)
    print("Fusing deps", deps)
    deps = _fuse_deps(deps, job_fusing_dic)
    print("Fusing deps Done", deps)
    print("Sequence fusing")
    _fuse_sequence_jobs(jobs, deps)
    print("Sequence fusing Done", deps)
    print("Renaming jobs")
    new_job_names=_get_jobs_names(jobs,deps)
    jobs, deps = _rename_jobs(jobs, deps, new_job_names)
    print("Renaming jobs Done")
    manifest_dic=_encode_manifest_dic(jobs, deps)
    f_out=open(json_route, "wb")
    json.dump(manifest_dic, f_out)   
    f_out.close()
        

def _encode_manifest_dic(jobs, deps):
    manifest_dic = {}
    manifest_dic["tasks"] = _produce_tasks(jobs)
    manifest_dic["resource_steps"] = _produce_resource_steps(jobs, deps)
    manifest_dic["max_cores"] = max([step["num_cores"] 
                                    for step in manifest_dic["resource_steps"]])
    
    manifest_dic["total_runtime"] = manifest_dic["resource_steps"][-1][
                                                                    "end_time"]
    manifest_dic["dot_dag"] = _produce_dot_graph(jobs, deps)
    return manifest_dic

"""
Functions to produce manifest elements.
"""
def _produce_dot_graph(jobs, deps):
    G=pgv.AGraph(directed=True)
    for job in jobs:
        G.add_node(job["id"])
    for (src, dst_list) in deps.items():
        for dst in dst_list:
            G.add_edge(src, dst)
    return G.to_string()
    
def _produce_tasks(jobs):
    tasks = []
    for job in jobs:
        task = {"id":job["id"],
                "number_of_cores":job["cores"],
                "name":job["id"],
                "execution_cmd":"./{0}.py".format(job["id"]),
                "runtime_limit":job["runtime"]+60,
                "runtime_sim":job["runtime"]
                }
        tasks.append(task)
    return tasks

def get_inverse_deps(deps):
    inverse_deps = {}
    for (src, dst_list) in deps.items():
        for dst in dst_list:
            if not dst in list(inverse_deps.keys()):
                inverse_deps[dst] = []
            inverse_deps[dst].append(src)
    return inverse_deps

def _produce_resource_steps(jobs, deps):
    resource_steps=[]
    
    inverse_deps = get_inverse_deps(deps)
 
    for job in jobs:
        job["completed"] = False
        job["src"]=[]
        if not "dst" in list(job.keys()):
            job["dst"]=[]
        
        if job["id"] in list(inverse_deps.keys()):
            for src_job in jobs:
                if src_job["id"] in inverse_deps[job["id"]]:
                    job["src"].append(src_job)
                    if not "dst" in list(src_job.keys()): 
                        src_job["dst"] = []
                    src_job["dst"].append(job)
    
    current_jobs = [job for job in jobs if  job["src"] == []]
    current_time=0
    for job in current_jobs:
        job["start"] = current_time
        job["end"] = current_time+job["runtime"]
    
    while not current_jobs == []:
        current_cores = sum(job["cores"] for job in current_jobs)
        next_current_time = min(job["end"] for job in current_jobs)
        resource_steps.append({"num_cores":current_cores,
                               "end_time":next_current_time})
        ending_jobs=[job for job in current_jobs 
                        if job["end"]==next_current_time]
        remaining_jobs=[job for job in current_jobs 
                        if job["end"]>next_current_time]
        for ending_job in ending_jobs:
            ending_job["completed"]=True            
            for new_job in ending_job["dst"]:
                if (_job_can_run(new_job)):
                    new_job["start"]=next_current_time
                    new_job["end"]=new_job["start"]+new_job["runtime"]
                    remaining_jobs.append(new_job)
        current_jobs = remaining_jobs
    return resource_steps

def _job_can_run(job):
    if job["src"] == []:
        return True
    for src_job in job["src"]:
        if not src_job["completed"]:
            return False
    return True
        
        

"""
Functions to obtain and manipulate the job list and dependencies dict 
"""        
def _rename_jobs(jobs, deps, new_job_name):
    """Renames jobs (in a jobs list dependencies dict) according to the
    new_job_name dict mapping."""
    for job in jobs:
        job["id"] = new_job_name[job["id"]]
    
    new_deps = {}
    for (src,dst) in deps.items():
        new_deps[new_job_name[src]] = [new_job_name[x] for x in dst]
    
    return jobs, new_deps
        
        
    
    
def _get_jobs_names(jobs, deps):
    """Returns a list of dict {"job_id":"Si"}, where Si is the new id of job
    with id job_id.""" 
    starting_jobs=[]
    all_dest=[]
    for some_dep in list(deps.values()):
        all_dest+=some_dep
    all_dest = sorted(list(set(all_dest)))
    for job in jobs:
        job_id=job["id"]
        if not job_id in all_dest:
            starting_jobs.append(job_id)
    count=0
    new_ids={}
    
    while not starting_jobs == []:
        next_step_jobs=[]
        for job_id in starting_jobs:
            if not job_id in sorted(list(new_ids.keys())):
                new_ids[job_id]="S{0}".format(count)
                count+=1
                if job_id in sorted(list(deps.keys())):
                    next_step_jobs+=deps[job_id]
        next_step_jobs=sorted(list(set(next_step_jobs)))
        starting_jobs=next_step_jobs
    return new_ids

def get_tag(tag, namespace=None):
    if namespace is None:
        return tag
    else:
        return "{"+namespace+"}"+tag

def _get_namespace(xml_wf):
    root_node = xml_wf.getroot()
    tag = root_node.tag
    pos1  = tag.find("{")
    pos2  = tag.find("}")
    if (pos1!=-1 and pos2!=-1):
        return tag[pos1+1:pos2]
    return None
def get_runtime(cad):
    cad=cad.replace(",",".")
    return float(cad)
def _get_jobs_and_deps(xml_wf, namespace=None):
    """Returns a list of jobs (dicts), and a dict containing their dependencies. 
    """ 
    root_node = xml_wf.getroot()
    if root_node.tag!=get_tag("adag", namespace=namespace):
        raise ValueError("XML dag format: unexpected root node {0}".format(
                         root_node.tag))

    jobs = []
    deps = {}
    total=len(root_node)
    count=0
    for child in root_node:
        if child.tag==get_tag("job", namespace=namespace):
            atts = child.attrib
            num_cores=1
            if "cores" in list(atts.keys()):
                num_cores=int(atts["cores"])
            jobs.append({"id": atts["id"],
                        "name": atts["name"],
                        "runtime":get_runtime(atts["runtime"]),
                        "cores":num_cores})

        if child.tag==get_tag("child", namespace=namespace):
            dest_job=child.attrib["ref"]
            for dep_origin in child:
                origin_job=dep_origin.attrib["ref"]
                try:
                    the_job_dep=deps[origin_job]
                except:
                    the_job_dep = []
                    deps[origin_job]=the_job_dep
                the_job_dep.append(dest_job)
        count+=1
        progress=count*100/total
        sys.stdout.write("Processed: %d%%   \r" % (progress) )
        sys.stdout.flush()
    return jobs, deps
    
def _reshape_job(job, max_cores):
    total_tasks=job["task_count"]
    cores_per_task=job["cores"]/job["task_count"]
    max_cores=(max_cores/cores_per_task)*cores_per_task
    
    runtime_per_task=job["acc_runtime"]/job["task_count"] 
    tasks_per_step=max_cores/cores_per_task
    
    new_runtime=0
    while total_tasks>0:
        new_runtime+=runtime_per_task
        total_tasks-=tasks_per_step
    
    return new_runtime, max_cores
    
def _fuse_sequence_jobs(jobs, deps):
    """Fuses any two jobs a, b if a.cores==b.cores and b only depends on a"""
   
    
    no_changes=False
    while not no_changes:
        no_changes=True
        inverse_deps = get_inverse_deps(deps)
        for (job_dst,dep_list) in inverse_deps.items():
            if len(dep_list)==1:
                job_orig=dep_list[0]
                if (len(deps[job_orig])==1 and 
                    _get_job(jobs, job_orig)["cores"] ==
                    _get_job(jobs, job_dst)["cores"]):
                    _fuse_two_jobs_sequence(jobs, deps, job_orig, job_dst)
                    no_changes=False
                    break

def _fuse_two_jobs_sequence(jobs, deps, job_orig, job_dst):
    the_job_orig=_get_job(jobs, job_orig)
    the_job_dst=_get_job(jobs, job_dst)
    
    the_job_orig["runtime"]+=the_job_dst["runtime"]
    jobs.remove(the_job_dst)

    if job_dst in list(deps.keys()):
        deps[job_orig]=deps[job_dst]
        del deps[job_dst]
    else:
        del deps[job_orig]
    
def _get_job(job_list, job_id):
    for job in job_list:
        if job_id==job["id"]:
            return job
    print("JOB not found", job_id)     
    return None

def extract_groups_cores(grouped_jobs, max_cores=None):
    """Processes the list of task names to group, extracting the max number of
    cores per task. The grouped task name can have the format:
    task_name:num_cores or just task_name. If num_cores is set, the max_cores
    is used instead.
    Args:
    - grouped_jobs: list of strings of the format task_name or
        task_name:num_cores. Task_name refers to the name field of tasks to 
        be grouped. num_cores to the maximum number of cores assigned to that
        tasks group.
    - max_cores: maximum number of cores used to group tasks in no specific
        value is set.
    Returns 
    - new_groups: list of names of grouped tasks.
    - max_cores_dic: dictionary of integers with the maximum number of cores
        per grouped tasks indexed by the name of grouped tasks.
    
    """
    new_groups = []
    max_cores_dic = {}
    for gr in grouped_jobs:
        if ":" in gr:
            tokens = gr.split(":")
            new_groups.append(tokens[0])
            max_cores_dic[tokens[0]]=(int(tokens[1]))
        else:
            new_groups.append(gr)
            max_cores_dic[gr]=max_cores
    return new_groups, max_cores_dic

def _fuse_jobs(job_list, grouped_jobs, max_cores=None):
    """Fuses jobs with the same name if name in grouped_jobs. Returns the new
    job_list, and a dictionary mapping the new job_id to fused job_ids"""
    job_dic={}
    new_job_list = []
    job_fusing_dic= {}
    grouped_jobs, max_cores_dic=extract_groups_cores(grouped_jobs, max_cores)
    for job_name in grouped_jobs:
        job_fusing_dic[job_name]=[]
          
    for job in job_list:
        job_name=job["name"]
        the_job = None
        if job_name in grouped_jobs:
            if job_name in list(job_dic.keys()):
                the_job=job_dic[job_name]
            if the_job is None:
                the_job = dict(job)
                the_job["cores"] = 0
                the_job["id"]=job_name
                the_job["acc_runtime"]=0
                the_job["task_count"]=0
                new_job_list.append(the_job)
                job_dic[job_name]=the_job
            job_fusing_dic[job_name].append(job["id"])
            the_job["cores"]+=job["cores"]
            the_job["runtime"]=max(the_job["runtime"], 
                                   job["runtime"])
            the_job["acc_runtime"]+=job["runtime"]
            the_job["task_count"]+=1
        else:
            new_job_list.append(job)
    for (job_name, the_job) in job_dic.items():
        this_max_cores=max_cores
        if job_name in list(max_cores_dic.keys()):
            this_max_cores=max_cores_dic[job_name]
        if this_max_cores:
            the_job["runtime"], the_job["cores"]=_reshape_job(the_job,
                                                            this_max_cores) 
        del the_job["acc_runtime"]
        del the_job["task_count"]
    
    return new_job_list, job_fusing_dic

def _fuse_deps(dep_dic, job_fusing_dic):
    """Fuses dependencies according to the fuse map in job_fusing_dic."""    
    inverse_fusing_dic={}
    for (new_dep, older_deps) in job_fusing_dic.items():
        for dep in older_deps:
            inverse_fusing_dic[dep] = new_dep
    
    new_dep_dict={}
    # first transform the sources and join the dest
    for (source, dest) in dep_dic.items():
        new_dest=[]
        for one_dest in dest:
            try:
                one_dest=inverse_fusing_dic[one_dest]
            except:
                pass
            new_dest.append(one_dest)
        dest=list(set(new_dest))  
   
        try:
            source = inverse_fusing_dic[source]
        except:
            pass
        
        try:
            new_dep_dict[source] += dest
            new_dep_dict[source]=list(set(new_dep_dict[source]))
        except:
            new_dep_dict[source] = dest
    return new_dep_dict
        