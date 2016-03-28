#!/bin/bash

# port number to use in the listening socket
port=10001

# port for communication with master node
master_port=10002

# ip list of peers
ip_file='ip.txt'

# input path list
input_list='inputs.txt'

while IFS='' read -r line || [[ -n "$line" ]]; do
# start the program on each slave
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$line "java -jar ~/Job.jar ${port} ${master_port} ${ip_file} ${input_list}  > ~/log 2>&1 &"
    echo "Node start working: $line"
done < "$1"


# waiting for the all the slave finishing their jobs
tmstate='done'
output='not yet'
while [ "$output" != "$tmstate" ]; do
	output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
	echo Waiting for job completion...
	sleep 1m
done

echo Sorting complete!
