HOSTS_FILE="./hosts.list"

while IFS='' read -r worker || [[ -n "$worker" ]]; do
	ssh -t $worker exit  &> /dev/null < /dev/null
	if [ $? = 0 ]; then
		echo "$worker Up"
	else
		echo "$worker Down"
	fi
done < "$HOSTS_FILE"
