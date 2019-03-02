TMP_FILE=/tmp/running.log
HOSTS_FILE="./hosts.list"
RUN_CMD="python ./run_sim_exp.py"
LOG_FOLDER="./log"
export PYTHONUNBUFFERED=1
ps aux | grep "$RUN_CMD" | grep -v "grep" | tr -s " " | cut -d\  -f13 > $TMP_FILE


cat "$HOSTS_FILE" | grep -v "!" | while IFS='' read -r worker || [[ -n "$worker" ]]; do
	echo "Checking... $worker"
	found=0
	while IFS='' read -r client || [[ -n "$client" ]]; do
		if [ "$client" = "$worker" ]; then
			echo "Worker $worker already running, skipping."
			found=1
			break
		fi
	done < "$TMP_FILE"
	if [ $found = 0 ]; then
		echo "Launching worker on $worker..."
		$RUN_CMD $worker &> "${LOG_FOLDER}/worker.log.$worker" &
	fi
done
