from math import ceil


def filler(workload_gen, start_time, target_wait, max_cores, cores_per_node, 
           job_separation=10, final_gap=120):
    time_stamp=start_time-target_wait-final_gap
    num_jobs = int(target_wait/job_separation)
    cores_per_job=int(ceil(float(max_cores/cores_per_node)/float(num_jobs)) * 
                      cores_per_node)
    
    for i in range(num_jobs):
        workload_gen._generate_new_job(
                            time_stamp,
                            cores=cores_per_job,
                            run_time=2*target_wait+final_gap,
                            wc_limit=int((2*target_wait+final_gap)/60+1),
                            override_filter=True)
        time_stamp+=job_separation
    
    