

from generate.pattern import PatternGenerator, PatternTimer
from math import ceil

class FixedJobGenerator(PatternGenerator):
    """Pattern generator that will always produce a job with the same runtime,
    allocated umber of cores, and wall_clock limit"""
    def __init__(self,workload_generator, cores, run_time, wc_limit=None):
        """Args:
        - workload_generator: WorkloadGenerator class that will receive the
            job creation actions.
        - cores: number if cores that all jobs must allocate.
        - run_time: seconds that jobs will run.
        - wc_limit: if set, wall clock limit in minutes for jobs. If not set,
            it is rounded up run_time in minnutes.
        """
        super(FixedJobGenerator, self).__init__(workload_generator)
        self._cores=cores
        self._run_time=run_time
        self._wc_limit=wc_limit
        if self._wc_limit is None:
            self._wc_limit=int(ceil(float(self._run_time)/60.0)+1)
        
    def do_trigger(self, create_time):
        """This method is called by the workload generator when it is time for
        this submission pattern to happen. It must use add_job to submit new
        jobs and return the number of jobs that it has submitted."""
        self._workload_generator._generate_new_job(create_time,self._cores,
                                                   self._run_time,
                                                   self._wc_limit)
class SpecialGenerators:
    """Factory class to produce Special Generators"""
    @classmethod
    def get_generator(cls, string_id, workload_generator,
                      register_timestamp=None, register_datetime=None):
        tokens = string_id.split("-")
        if tokens[0] != "sp":
            raise ValueError("sting_id does not start with sp: {0}".format(
                                                                   string_id))
        if tokens[1] == SaturateGenerator.id_string:
            gen = SaturateGenerator(workload_generator, register_timestamp,
                                    register_datetime)
            gen.parse_desc(string_id)
            return gen
        elif tokens[1] == BFSaturateGenerator.id_string:
            gen = BFSaturateGenerator(workload_generator, register_timestamp,
                                    register_datetime)
            gen.parse_desc(string_id)
            return gen
        
        raise ValueError("Unkown generator string_id {0}".format(string_id))

class SaturateGenerator(PatternTimer):
    """Pattern timer that produces a fixed number of identical jobs, submitted
    with the same inter-arrival time (a blast). It will submit a blast every
    a fixed number of seconds. A ne blast cannot be configured to start until
    the previous one is finished.
    
    """
    id_string = "sat"
    def __init__(self, workload_generator, register_timestamp=None,
                 register_datetime=None):
        super(SaturateGenerator,self).__init__(None,register_timestamp,
                                          register_datetime)
        self._workload_generator = workload_generator
        self._workload_generator.disable_generate_workload_elemet()
   
    def parse_desc(self, desc_cad):
        """Configures the behavior of the generator from the content of a string
        like: sp-sat-p1-c24-r36000-t5576-b30424. Format:
        - pX: number of seconds between jobs >0.
        - cX: number of cores allocated by each job
        - rX: runtime in seconds of each job.
        - tX: number of jobs per blast.
        - bX: number of seconds in each blast.
        contraint bX>pX*tX
        """
        tokens = desc_cad.split("-")
        self._job_period=int(tokens[2].split("p")[1])
        self._cores = int(tokens[3].split("c")[1])
        self._run_time = int(tokens[4].split("r")[1])
        self._jobs_per_blast = int(tokens[5].split("t")[1])
        self._blast_period = int(tokens[6].split("b")[1])
        
        if self._blast_period<self._jobs_per_blast*self._job_period:
            raise ValueError("Blast period is too short, a new blast cannot "
                             "start before the previous one ends.")
        
        self._pattern_generator=FixedJobGenerator(self._workload_generator,
                                                  cores=self._cores,
                                                  run_time=self._run_time)
        
    def register_time(self, timestamp):
        super(SaturateGenerator,self).register_time(timestamp)
        
        self._configure_blast(timestamp+1)
    
    def _configure_blast(self, blast_starts_at):
        self._next_blast_time=blast_starts_at
        self._jobs_submitted=0
        self._next_job_time=blast_starts_at
        
    def _submit_jobs_blast(self, stamp):
        this_call_jobs_sub=0
        while (self._jobs_submitted<self._jobs_per_blast and
               self._next_job_time<=stamp):
            self._pattern_generator.do_trigger(self._next_job_time)
            self._next_job_time+=self._job_period
            self._jobs_submitted+=1
            this_call_jobs_sub+=1
        if self._jobs_submitted==self._jobs_per_blast:
            self._configure_blast(self._next_blast_time+self._blast_period)
        return this_call_jobs_sub
    
    def do_trigger(self, create_time):
        return self._submit_jobs_blast(create_time)       
            
    def can_be_purged(self):
        return False
    
class BFSaturateGenerator(SaturateGenerator):
    """Pattern timer that produces a repeating job submissiong pattern to
    saturate a system by backfilling jobs: One Long job (j1), one wide job (j2)
    that cannot start until j1 ends. Then a set of smaller jobs that would fit
    before j2 without delaying it, and fill the machine to 100%T utilization.
    
    """
    id_string = "bf"
    
    def parse_desc(self, desc_cad):
        """Configures the behavior of the generator from the content of a string
        like: sp-sat-p10-c24-r61160-t5756-b123520-g600-lc240-lr119920-wc133824-wr3600.
        Format:
        - pX: number of seconds between jobs >0.
        - cX: number of cores allocated by each job
        - rX: runtime in seconds of each job.
        - tX: number of jobs per blast.
        - bX: number of seconds in each blast.
        - g: gap between the small jobs and the wide job
        - lc: number of cores of the long job
        - lr: runtime in seconds of the long job
        - wc: number of cores of the wide job
        - wr: runtime in seconds of the wide job 
        contraint bX>lr+wr
        """
        tokens = desc_cad.split("-")
        self._blast_period = int(tokens[6].split("b")[1])
        self._gap =  int(tokens[7].split("g")[1])
        self._long_cores =  int(tokens[8].split("lc")[1])
        self._long_runtime =  int(tokens[9].split("lr")[1])
        self._wide_cores =  int(tokens[10].split("wc")[1])
        self._wide_runtime =  int(tokens[11].split("wr")[1])
        
        if self._blast_period<self._long_runtime+self._wide_runtime:
            raise ValueError("Blast period is too short, a new blast cannot "
                             "start before the previous one ends.")
        
        
        self._long_job_generator=FixedJobGenerator(self._workload_generator,
                                                  cores=self._long_cores,
                                                  run_time=self._long_runtime)
        self._wide_job_generator=FixedJobGenerator(self._workload_generator,
                                                  cores=self._wide_cores,
                                                  run_time=self._wide_runtime)

        
        self._small_jobs_generator = SaturateGenerator(self._workload_generator,
                                                       self._register_timestamp)
        self._small_jobs_generator.parse_desc(desc_cad)
        
    def register_time(self, timestamp):
        if hasattr(self, "_small_jobs_generator"):
            self._small_jobs_generator.register_time(timestamp)
        super(BFSaturateGenerator,self).register_time(timestamp)
        if hasattr(self, "_small_jobs_generator"):
            self._small_jobs_generator._configure_blast(timestamp+120+1)        
    
    def _configure_blast(self, blast_starts_at):
        self._next_blast_time=blast_starts_at
        self._long_job_submit_time=blast_starts_at
        self._wide_job_submit_time=blast_starts_at+60
                
        
        
    def _submit_jobs_blast(self, stamp):
        
        
        this_call_jobs_sub= 0
        
        if (self._long_job_submit_time is not None and
            stamp>=self._long_job_submit_time):
            self._long_job_generator.do_trigger( self._long_job_submit_time)
            self._long_job_submit_time = None
            this_call_jobs_sub+=1
        if (self._wide_job_submit_time is not None and
             stamp>=self._wide_job_submit_time):
            self._wide_job_generator.do_trigger( self._wide_job_submit_time)
            self._wide_job_submit_time  = None
            this_call_jobs_sub+=1
            self._configure_blast(self._next_blast_time+self._blast_period)   
        this_call_jobs_sub+=self._small_jobs_generator._submit_jobs_blast(stamp)
              
        return this_call_jobs_sub
    
   