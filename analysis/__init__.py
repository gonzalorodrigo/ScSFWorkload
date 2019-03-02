"""
ProbabilityMap stores in memory, dump to disk, and load from disk probability
 maps.  
"""
from bisect import bisect_right, bisect_left
import pickle
import random
import random_control

class ProbabilityMap(object):
    """
    This Class can be configured to produce random numbers controlling the
    probability for the produce value to be in an specific range.
    """
    def __init__(self, probabilities=None, value_ranges=None, 
                 interval_policy="random", value_granularity=None, 
                 round_up=False):
        """
        Creation method.
        Args:
        - probabilities: List of accumulated probabilities of a value to 
            belong to each  element in value_ranges or the previous ones.
        - value_ranges: range of values list that this class can produce.
        - interval_policy: policy to produce the value within a range. Possible
            options:
                "random": uniform random number within the range
                "midpoint": middle value of the interval.
                "low": smaller number of the interval
                "high": bigger value of the interval
        - value_granularity: If set with a natural, the produced numbers will be
            multiple of value_granularity. Rounding is controlled by round_up
        - round_up: if false, rounding is floor, if True, celling.
            
        """
        if probabilities is not None and value_ranges is not None:
            if len(value_ranges)!=len(probabilities):
                raise ValueError("Value ranges must have the same number of "
                                 "elements than "
                                 "probabilities")
            
            for one_interval in value_ranges:
                if not len(one_interval)==2:
                    raise ValueError("Value ranges have to be tuples: {}".format(
                                                                one_interval))
                if one_interval[1]<one_interval[0]:
                    raise ValueError("Wrong Tuple: {}".format(one_interval))
            
            prev_val=None
            
            for prob in probabilities:
                if (prev_val is not None):
                    if prev_val>prob:
                        raise ValueError("Probabilities don't increase")
                if prob<0 or prob>1.1:
                    raise ValueError("A single probability cannot be under 0 or"
                                     " over 1: {}".format(prob))
                prev_val=prob
            if probabilities[-1]<0.99:
                raise ValueError("maximum probability is not present")
            probabilities[-1]=1.0

            
        self._container = dict(probabilities=probabilities,
                               value_ranges=value_ranges,
                               value_granularity=value_granularity,
                               interval_policy=interval_policy,
                               round_up=round_up)
        
        self.random_gen = random_control.get_random_gen()
    
    def get_probabilities(self):
        return self._container["probabilities"]
    
    def get_value_ranges(self):
        return self._container["value_ranges"]
    
    def get_value_granularity(self):
        return self._container["value_granularity"]
    
    def get_interval_policy(self):
        return self._container["interval_policy"]
    
    def get_round_up(self):
        return self._container["round_up"]
        
    def save(self, file_route):
        """Saves the content in file_route"""
        output = open(file_route, 'wb')
        pickle.dump(self._container, output);
        output.close()
    
    def load(self, file_route):
        """Fills the object with the content in file_route"""
        pkl_file = open(file_route, 'rb')
        self._container = pickle.load(pkl_file)
        pkl_file.close()
        
    def produce_number(self):
        """Produce a random number according to the configuration."""
        if not self.get_probabilities() or not self.get_value_ranges():
            raise ValueError("Probability not configured correctly")
        r=self.random_gen.uniform(0.0, 1.0)
        value_interval = self._get_range_for_number(r)
        n = self._get_value_in_range(value_interval, self.get_interval_policy())
        n = _round_number(n,self.get_round_up())
        return n
        
    def _get_range_for_number(self, number):
        """Returns the value range corresponding to the probability number"""
        if number<0 or number>1:
            raise ValueError("number({0}) has to be in [0, 1]".format(number))
        position = bisect_left(self.get_probabilities(), number)
        return self.get_value_ranges()[position]
    
    def _get_value_in_range(self, value_range, policy="random"):
        """Returns a value within a value range. It supports different policies.
            Args:
                - value_range: tuple of floats representing an interval of
                    values in which the output value should be contained.
                - policy: string configuring how the output value is chosen. 
                    Possible values:
                    - "random": output value will be a random one in value range
                        (uniform)
                    - "midpoint": linear center of the value range.
                    - "low": smaller possible value in value_range
                    - "high": bigger possible value in value_range
                    - "absnormal": positive values of normal distribution,
                         mean=value_range[0] and stdev=0.1.
            Returns: a number within value_range according to policy
        """
        if policy == "random":
            r=self.random_gen.uniform(
                                  float(value_range[0]), 
                                  float(value_range[1]))
            return r
        if policy == "midpoint":
            return value_range[0]+((float(value_range[1]) - 
                                    float(value_range[0]))/2)
        if policy == "low":
            return value_range[0]
        
        if policy == "high":
            return value_range[1]
        if policy == "absnormal":
            r=abs(self.random_gen.normalvariate(0.0,0.1))
            r=min(1.0, r)
            return ((float(value_range[1])-float(value_range[0]))*r +
                         float(value_range[0]))
        
        raise ValueError("Undefined interval policy: "+str(policy))
        
def _round_number(n, value_granularity=None, up=False):
    if not value_granularity:
        return n
    else:
        if not up:
            return n-(n % value_granularity)
        else:
            return n-(n % value_granularity) + value_granularity
        
        


    