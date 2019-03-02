#!/bin/bash
function my_test {
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error executing: $@"
	echo "Exiting"
	exit -1
    fi
}
SLURM_FOLDER="~/slurmsimdeploy"

worker_ip=$1
branch_name=$2
echo "About to compile SLURM in ${worker_ip}, branch $branch_name"
cmd1="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm; git fetch"
my_test $cmd1
cmd2="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm; git checkout $branch_name"
my_test $cmd2
cmd3="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm; git pull origin $branch_name"
my_test $cmd3
cmd4="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm; make clean"
my_test $cmd4
cmd5="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm; make"
my_test $cmd5
cmd6="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm; make -j install"
my_test $cmd6
cmd7="ssh -A -t ${worker_ip} cd ${SLURM_FOLDER}/slurm/contribs/simulator; make -j install"
my_test $cmd7
echo "Compilation done!"
