from .pattern import PatternTimer


class OverloadTimeController(PatternTimer):
    """This PatternTimer is created to overload the system that is fed the
    workload. It will submit a number of jobs per second while the total
    submitted core hours are around overload_target times the system capacity
    for the time that jobs have been submitted so far. It configures a 10 
    minutes decay window to observe past jobs. It uses a hysteresis controller
    to avoid oscillation using 5% as the radius for it.
    """
    
    def configure_overload(self, trace_generator, capacity_cores_s,
                           overload_target=0.0):
        """Configure special parameters for the overload.
        Args:
        - trace_generator: TraceGenerator object used by the wrapping generator.
        - capacity_core_s: int representing the cores per second produced by the
            target system of the workload.
        - jobs_per_second: jobs to produce per second.
        - overload_target: how many times the capacity of the system in a period
            of time should be submitted.
        """
        self._trace_generator=trace_generator
        self._capacity_cores_s=capacity_cores_s
        self._overload_target=overload_target
        
        self._decay_window_size=3600
        self._hysteresis_radius=0.05
        
        self._trace_generator.set_submitted_cores_decay(self._decay_window_size)
        self._hysteresis_trend=0
        
    
    def is_it_time(self, current_timestamp):
                
        super(OverloadTimeController, self).is_it_time(current_timestamp)
        
        submitted_core_s, runtime = self._trace_generator.get_submitted_core_s()
        
        pressure_index=(float(submitted_core_s) / 
                      float(self._capacity_cores_s*runtime))
        
        if pressure_index<self._overload_target:
            return 1
        return 0
    
    def can_be_purged(self):
        return False
    
    def do_reentry(self):
        return True
    
    
    