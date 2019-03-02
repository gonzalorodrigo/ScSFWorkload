"""
Pattern generator that induces a share of core hours to workflows and 1-share
to regular jobs. Supports an upper_cap, limiting the core hours over produced
work that the workload admits.

It is a version of the random generator in which the decussion of the generator
returned is not random but governed by the number of regular and workflow
core hours present in the workload.
"""

from generate import RandomSelector

class WorkflowPercent(RandomSelector):
    
    def __init__(self, random_gen, trace_gen, time_controller, total_cores,
                 max_pressure=None):
        """Constructor.
        Args:
            - random_gen: is a Random generator object that has a
                uniform(float, float) method that returns a float 
                between the two.
            - trace_gen: trace generator that is being used by the workload
                generator and admit to check how many core hours of jobs
                and workflows have been generated.
            - time_controller: time controller used by the workload generator.
                It provides how long the workload has been running at this 
                point of workload generation.
            - total_cores: integer with the number of cores present in the 
                system which workload is produced for.
            - max_pressure: float, if set, the core hours in the workload
                will be limited to: max_pressure*total_cores*runtime
        """
        super(WorkflowPercent, self).__init__(random_gen)
        self._trace_gen = trace_gen
        self._time_controller=time_controller
        self._total_cores=total_cores
        if max_pressure is None:
            max_pressure = 1.1
        self._max_pressure = 1.1
        
    
    def _get_pressure_index(self):
        total_runtime = self._time_controller.get_runtime()
        total_core_s =  self._trace_gen.get_total_actual_cores_s()
        pressure_index = 0
        if (total_runtime>=3600): 
            pressure_index = (float(total_core_s) /
                              float(self._total_cores*total_runtime))
        return pressure_index
        
    def get_random_obj(self):
        """Gets a random obj according to %share configured. """
        if len(self._prob_list)==2:
            # we get the share and check the workflow core hours
            if self._get_pressure_index() >self._max_pressure:
                return None
            workflow_share=self._prob_list[1]-self._prob_list[0]
            acc_wf_share=self._trace_gen.get_share_wfs()
            if (acc_wf_share is not None) and (acc_wf_share < workflow_share):
                return self._obj_list[1]
            else:
                return self._obj_list[0]
                    
        
        return super(WorkflowPercent, self).get_random_obj()
    
    def config_upper_cap(self, upper_cap):
        self._max_pressure=upper_cap
    
    