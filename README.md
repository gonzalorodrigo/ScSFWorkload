# ScSF: Scheduling Simulation Framework

ScSF is a scheduling simulation framework that includes a set of tools that
enables scheduling research. It was developed by the
Data Science and Technology department (DST) at the Lawrence Berkeley National
Lab and Distributed System group at Umeå University. For questions, feedback,
and bug-reporting, please contact its main contributor (Gonzalo Rodrigo) 
at gprodrigoalvarez@lbl.gov 


ScSF is a complete framework with functions to:

- Model workloads.
- Generate workloads according to a model.
- Run those workloads through an scheduler simulator.
- Retrieve simulation results and perform analysis.
- Coordinate the concurrent execution of multiple simulations.
- Analyze and compare simulation results.
- Run experiments with workflows and different workflow submission strategies.

This read-me includes information to install, configure, and operate
ScSF. In case of doubt, the code is the best reference on how to use the
framework.

If you use or extend this work, please add a citation to:
R. Gonzalo P., E. Elmroth, P-O. Ostberg, and L. Ramakrishnan, ScSF: a
Scheduling Simulation Framework', in Proceedings of the 21th Workshop on Job
Scheduling Strategies for Parallel Processing, 2017 ( [link](http://www.jsspp.org/papers17/paper_2.pdf) )

## Installation and worker(s) setup

### Worker setup

The package includes the control code of ScSF. ScSF requires the presence
of at least one ScSF worker to function i.e., a host or VM in which the Slurm Worker
code has been deployed. A tar.gz with the code and instructions to configure
a slurm worker can be found at [http://frieda.lbl.gov/download](http://frieda.lbl.gov/download)

### ScSF installation

ScSF requires a MySQL database to operate. The package include scripts to 
create and configure the required database. Steps:

- Configure MySQL to accept large SQL queries. In my.cnf:
~~~
max_allowed_packet=128M
~~~


- Install in a virtual environment.
~~~
# Omit next code block if no virtual environment is required.
virtualenv env
source env/bin/activate

pip install -r requirements.txt
python setup.py install
~~~

Note: In OS X installation of pygraphviz might file. In that case, to install
pygraphviz run ([More info](http://www.alexandrejoseph.com/blog/2016-02-10-install-pygraphviz-mac-osx.html):
~~~
pip install pygraphzviz \
--install-option="--include-path=/usr/local/include/graphviz/" \
--install-option="--library-path=/usr/local/lib/graphviz"
~~~

- Create database and default users:
~~~
bin/sql_createdb.sh
~~~
The script uses root user to access the database and its password is requested
twice.

- Create database schema (assuming default database users)
~~~
cd bin
# Configure env vars read by ScSF to configure its database access.
source sql_conf_env.sh
# Create schema
python sql_populate_db.py
~~~

- Compile list_trace command. This step requires the presence of the
Slurm Worker package with its Slurm code downloaded and patched. Assuming that
Slurm Worker package root folder is "/somefolder/slurmsimdeploy", run:
~~~
compile_list_trace.sh /somefolder/slurmsimdeploy
~~~


- Basic testing after installation.
~~~
source bin/sql_conf_env.sh
cd tests
./test_all.sh

#If a ScSF worker is present 
export TEST_VM_HOST="hostnameOfWorker"
./test_all_vm.sh
~~~

## Operating the simulator

### A target system model

ScSF requires a workload and system model to operate. A model is composed of:

- A machine derived class in the python machines library.
- Workload generation data to reproduce the original system's workload.
- A hardware system definition in a Slurm's slurm.conf.

The package includes a sample model of an HPC system to help users to 
familiarize with ScSF. This model has similar hardware configuration to
NERSC's Edison in terms of number of nodes, cores per node, and memory per node
(as described in [Edison's configuration page](http://www.nersc.gov/users/computational-systems/edison/configuration/)).
However, the workload model does not correspond to a real workload.

#### Modeling a new system

0. Define a string that will identify the model, e.g. "newsystem"

1. Create a new Machine class in machines/__init__.py. The class should be
similar to Edison2015, eg.. NewSystem:

    - Define the number of nodes in the system: num_nodes in parent init
    - Include a load_from_file to load the future model files.
    - Define specific functions adjust the model behavior.

2. Add an entry in the orchestration.ExperimentDefinition.get_machine() so the
"newsystem" string in the experiment definition is associated to The NewSystem
machine class.

3. Create a base slurm.conf file that represents your system. The key factors
are the number of nodes, cores per node, etc. Use the included slurm.conf files
as reference. It should be placed in the bin/configs folder with the name:
slurm.conf.newsystem.regular. 

4. Model the workload. Assuming the existence of a representative HPC workload,
ScSF can parse it and generate a model using the empirical distribution. For an
example of how to do this read: bin/model_system_workload.py

The "gen" files generated by the modeler should be placed in the bin/data 
folder with names that match the code in the class init definition.
 
### Experiments

Experiments in ScSF are defined by: 

- A reference system model: Workload, hardware definition, and Slurm
configuration.
- Workload generation parameters.
- Experiment runtime.

The life cycle of an experiment starts at its definition, continues in the
execution of its simulation, and ends in the analysis of its results.
An extra stage is possible, to compare experiments that with small differences
in their experimental conditions.

Also, ScSF supports the concept of experiment groups: meta experiments that
group single experiments with the same conditions but a different random
generator initialization (random seed in the experiment definition).

#### Experiment states
Single experiments state change as the different parts of ScSF push them through
their life cycle:

~~~

ExperimentDefinition.store()
    |
---------
  fresh
---------
    |________ 
---------    |
pre_sim.  	 |
---------    |
---------	 |
simulating 	 | run_sim_exp.py
---------    |
---------	 |
completed 	 |
---------    |
	|________|
	|________
---------	 |
pre_analysis |
---------    |run_analysis_exp.py
---------	 |
analysis done|
---------    |
	|________|
	|
	|____________
---------	 	|
pre_second_pass |
---------    	|
	|		 	| run_analysis_second_pass.py
---------	 	|
sceond_pass_done|
---------    	|
	|___________|

~~~

Equivalent states and scripts are present in the grouped experiments: 
run_analysis_exp_group.py and run_analysis_exp_group_second_pass.py

### Experiment definition

The best reference is in the specification of the
orchestration.ExperimentDefinition class. In this section, we provide
a high level view of it.

#### Single experiments

Examples of experiment creation can be explored in: example_create_basic_exp.py.
A single experiment is defined using the orchestration.ExperimentDefinition
class and storing its values in the database. Configurable parameters include:

Details on the experiment definition can be read in the ExperimentDefinition
class. Its parameters allow to configure the workload generator (model selection
random seed), general trace settings (pressure, pre-estabilization period,
drain period), presence of workflows (types, frequency, submission mode),
duration, and Slurm configuration.

If not set, ScSF uses default configuration files to run the experiment.
However, specific configuration files can be created (for example to test
a particular scheduler configuration parameter), placed in the bin/configs
folder, and added to the conf_file field of an experiment.


#### Group experiments

Single experiments with the same experimental conditions but different random
seed might be considered repetitions of the same experiment. To analyze them
together a group experiment is created referring to them.

Examples of Group experiment creation can be found at:
bin/example_create_grouped_exp.py. In general group experiments include
a list of experiment ids of the experiments they group (sub-traces). The
experimental conditions should set to the same parameters as the single
experiments.

Their initial state is "pending" and cannot be analyzed until all the grouped
experiments are ins "completed" state.

#### Workflow specific experiment

ScSF was created to investigate workflow-scheduling algorithms and, as
consequence, it includes tools to add workflows to its workloads.Examples of
experiment definitions with workflows can be found in:
bin/example_create_workflow_exp.py.

Workflow experiments include extra definition information:

- Workflow Manifest: A JSON file defining the structure of the workflow, i.e., 
stages, resources and runtime of each stage, and dependencies. Manifest files
must be place in bin/manifests. This folder includes examples on the format.
Workflows can be write manually or generated with the bin/xml2json.py:
transforms XML workflow definitions from the [Pegasus workflow generator](https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator).

- Workflow presence in definition (i.e. Which workflows and How many):
Controlled by workflow_policy ("period""/"percentage"), workflow_period_s
(seconds between two workflows), and workflow_share (percentage of jobs that
are workflows). manifest_list is a list of the present manifests and share
(/1 of the workflows that correspond to each manifest).

- Workflow submission method: ScSF supports three ways of submitting workflows:
as a pilot job, one job per stage chained through dependencies, and "workflow
aware" (single job including the workflow manifest). The method can be selected
with the workflow_handling field in the definition. More details on the
workflow submission modes can be found at: [Enabling Workflow-Aware Scheduling
on HPC Systems](http://dl.acm.org/citation.cfm?id=3078604) .

The Workflow generator include functions to add programatic behavior: defining
python classes to generate special jobs of workflows. For more details read
about the "generate" package. 

### Running experiments 

ScSF uses ScSF workers to run experiments and the system can run as many
concurrent experiments as workers are setup. ScSF includes the run_sim_exp.py
script to control a worker. For example, two ScSF workers are running on
two hosts (or VMs) of IPs 192.168.56.101 and 192.168.56.101. In a third host
two commands are run:

~~~
python run_sim_exp.py 192.168.56.101 > log1.txt &
python run_sim_exp.py 192.168.56.102 > log2.txt &
~~~

Two worker managers are instantiated. Each one take control of worker to feed
experiments into them until no "pending" single experiments are left. Result
traces are stored in the ScSF database (traces table) for later analysis.

### Results analysis

Analysis can be performed in any system that has the ScSF code and can access
the central database.

#### Single experiments

Results analysis of single experiments can be done by running
bin/run_analsys_exp.py. Results are stored in the database in the form of:

- Job variables analysis: histogram, CDF, and percentile analysis of observed
requested runtime, actual runtime, runtime accuracy, allocated cores, wait time,
slowdown, and turnaround time.

- Workflow variables analysis: histogram, CDF, and percentile analysis, per 
manifest and overall of: wait time, runtime, turnaround time, and stretch factor.
It supports the three type of workflow submission policies.

- System utilization: integrated, median per minute/hour, and usage (utilization
minus idle allocation within workflows).

The scripts include code to discard initial (simulation ramp-up) and final
(drain cut-off) data in each simulation that is not representative and might
perturb the results is considered.

#### Group experiments

bin/run_analsys_exp_group.py produce similar results to the ones for single
experiments. However, analysis is performed over the aggregated traces of
all the experiments in the group. In order to run a group analysis all the
experiments of the group must be in completed state.

#### Comparing experiments

Experiments that differ only in the workflow submission method might differ
significantly on the number of run workflows. As a consequence workflow 
variables comparison requires as second pass analysis that analyze the
workflow variables on the first same number of workflows (minimum across each
trace).

This is done in the second pass analysis. bin/run_analysis_exp_second_pass.py 
scans for sets of experiments that have the same parameters (including seed)
but different workflow submission method. When all experiments in a set are in 
"analysis_completed" status, it performs the second pass. An analogous function
is present for experiment groups in bin/run_analysis_exp_second_pass_group.py.

### Results plotting

ScSF includes tools to plot histograms (profiles), CDFs, and boxplots from
experiment results. Additionally, scripts to combine and compare results are also
present:

- bin/plot_xx.py scripts allow to plot data of single experiments.
- bin/plit_rxamples: include scripts that indicate how to analyzed experiement
sets combined.

## Working in scale

Scheduling research requires repeating experiments under many experimental
conditions. As a consequence running experiments in scale is required.
This is possible in ScSF by adding more ScSF workers and running multiple
run_sim_exp.py instances.

ScSF provides a number of scripts and components to ease the management of ScSF 
workers:

- bin/hosts.list is a text file in which each line is the hostname/IP of a
ScSF worker. Operations on workers will be performed only on the ones listed
here.
- workers_launch.sh: launches a worker manager for each ScSF worker.
- workers_list.sh: lists current active workers.
- workers_ping.sh: checks that the ScSF workers are accesible via SSH.
- workers_kill.sh: stops all the worker controllers.
- workers_recompile.sh: forces recompilation of the code in the ScSF workers.
If the code in the worker is connected to a git repository it forces its
re-compilation.

### SShuttle, a useful tool for VM access

ScSF workers can be protected by running them in host-only networks (or isolated
from the Internet). However, that makes remote access from ScSF complicated.
In our work we used SSHuttle: an easy to use VPN over SSH for Linux and Mac OSX
(available in distro repos, brew, or https://github.com/apenwarr/sshuttle).
SShuttle allows to access the host-only network of a host via SSH. For example:

- Host1: Is a host of VMs, contains two ScSF worker VMs in the host only network 
192.168.100.x.
- Host2: Is the ScSF manager host. 

The following command run in host2:
~~~
nohup sshuttle -r user@host1 192.168.100.0/24 -vv > /dev/null 
~~~
Allows host2 to ssh, and thus connect, to the ScSF worker VMs transparently.

## Authoring and license

This code was authored by Gonzalo P. Rodrigo (gprodrigoalvarez@lbl.gov).
For license details read LICENSE file..

### Copyright

Scheduling Simulation Framework for HPC systems (ScSF) Copyright (c) 2017, The
Regents of the University of California, through Lawrence Berkeley National
Laboratory (subject to receipt of any required approvals from the U.S. Dept. of
Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department of
Energy and the U.S. Government consequently retains certain rights. As such, the
U.S. Government has been granted for itself and others acting on its behalf a
paid-up, nonexclusive, irrevocable, worldwide license in the Software to
reproduce, distribute copies to the public, prepare derivative works, and
perform publicly and display publicly, and to permit other to do so. 


 