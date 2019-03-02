TMP_FILE=/tmp/running.log
HOSTS_FILE="./hosts.list"
RUN_CMD="python ./run_sim_exp.py"
LOG_FOLDER="./log"
ps aux | grep "$RUN_CMD" | grep -v "grep" | tr -s " " | cut -d\  -f13 > $TMP_FILE

while IFS='' read -r worker || [[ -n "$worker" ]]; do
	found=0
	while IFS='' read -r client || [[ -n "$client" ]]; do
		if [ "$client" = "$worker" ]; then
			found=1
			break
		fi
	done < "$TMP_FILE"
	if [ $found = 1 ]; then
		echo "$worker"
	fi
done < "$HOSTS_FILE"
