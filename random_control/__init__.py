""" This package provides a global random generator to be able to run
experiments with determinsim.

All code using get_rand_gen will use the same random generator if
previously set_globral_random_ge has been used with one of its parameters
not None.

To repeat experiments, use set_global_random_gen before the experiment
with the same "seed" value.
"""

import random

global_random_gen = None

def set_global_random_gen(seed=None,random_gen=None):
    """Used by the whole package to get the random generator by all the
    code. Set it to the same seed initiated random object.
    Args:
        - seed: if set to a _hashable_ obj, a new random generator is
            created with seed as seed.
        - random_gen: Random object to be set as global. this argument
            is ignored if seed is set.
    """
    global global_random_gen
    if seed is not None:
        global_random_gen = random.Random()
        global_random_gen.seed(seed)
    else:
        global_random_gen = random_gen
        
def get_random_gen(seed=None):
    """Returns the global random generator if it is set, creates a new one
    otherwise. If seed is set it will be used for that purpose."""
    global global_random_gen
    if global_random_gen is not None:
        return  global_random_gen
    r = random.Random()
    r.seed(a=seed)
    return r