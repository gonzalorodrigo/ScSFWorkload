#!/bin/bash

HOSTS_FILE="./hosts.list"
LOG_FOLDER="./log"
BRANCH_NAME="oss-clean"
TMP_FILE="/tmp/compile_temp.txt"
MYA=()
WORKERS_PID=()
cat "$HOSTS_FILE" | grep -v "!" > "$TMP_FILE"
while IFS='' read -r worker || [[ -n "$worker" ]]; do
	echo "Recompiling $worker, branch $BRANCH_NAME"
	./do_recompile.sh $worker $BRANCH_NAME &> "${LOG_FOLDER}/compile.${worker}.log" &
	pid=$!
	MYA+=("$pid")
	WORKERS_PID["$pid"]="$worker"
done < "$TMP_FILE"
exit_status=0
for pid in "${MYA[@]}"; do
	echo "Waiting for compilation running on $pid to end..."
	wait $pid
 	status=$?
        if [ $status -ne 0 ]; then
		echo "Warning compilation on ${WORKERS_PID[$pid]} failed, check log" 
		exit_status=-1
        fi      
done
if [ $exit_status -eq 0 ]; then
	echo "All workers compiled correctly"
fi
exit $exit_status
