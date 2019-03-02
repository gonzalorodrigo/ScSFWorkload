#!/bin/bash
getpid () {
	echo `ps aux | grep "python ./run_sim_exp.py $1" | grep -v "grep" | tr -s " " | cut -d\   -f2`
}
getpid_subw() {
	echo `ps aux | grep "run_sim.sh" | grep "$1" | grep -v "grep" | tr -s " " | cut -d\   -f2`
}





TMP_FILE=/tmp/running.log
HOSTS_FILE="./hosts.list"
RUN_CMD="python ./run_sim_exp.py"
LOG_FOLDER="./log"
STOP_COMMAND="/home/gonzalo/cscs14038bscVIII/stop_sim.sh"

ps aux | grep "python ./run_sim_exp.py" | grep -v "grep" | tr -s " " | cut -d\  -f13  > $TMP_FILE

cat "$HOSTS_FILE" | grep -v "!" | while IFS='' read -r worker || [[ -n "$worker" ]]; do
	echo "Checking... $worker"
	if [ "$worker" = "" ]; then
		continue
	fi
	found=0
	while IFS='' read -r client || [[ -n "$client" ]]; do
		if [ "$client" = "$worker" ]; then
			echo "Worker $worker running."
			found=1
			break
		fi
	done < "$TMP_FILE"
	if [ $found = 1 ]; then
		worker_pid=`getpid $worker`
		echo "Killing worker $worker process $worker_pid."
		kill -9 $worker_pid
		ssh -A -t $worker $STOP_COMMAND &> /dev/null < /dev/null &
	fi
	subw_pid=`getpid_subw $worker`
	if [ "$subw_pid" != "" ]; then
		echo "Subworker for $worker is still alive with pid(s) $subw_pid"
		echo "Killing $subw_pid"
		kill -9 $subw_pid
	fi
done
