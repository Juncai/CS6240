#!/bin/bash

rm -rf logs > /dev/null 2>&1 &
mkdir logs > /dev/null 2>&1 &

# port number to use in the listening socket
port=10001

# port for communication with master node
master_port=10002

# ip list of peers
ip_file='address.txt'

# input path list
input_prefix='inputs_'

# ips[0]='some ip'
# counter
i="0"

while IFS='' read -r line || [[ -n "$line" ]]; do
# start the program on each slave
	scp -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$line:~/log logs/log_$i
	i=$[$i+1]
    echo "Log of node got: $line"
done < "$ip_file"

# i="0"
# for ip in "${ips[@]}"
# do
# 	echo "'java -jar ~/Job.jar $port $master_port $ip_file $i $INPUT_BUCKET $input_prefix$i $OUTPUT_BUCKET > ~/log 2>&1 &'"
# 	echo $ip$i
# 	i=$[$i+1]
# done

# waiting for the all the slave finishing their jobs
tmstate='done'
output='not yet'
# while [ "$output" != "$tmstate" ]; do
# 	output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
# 	echo Waiting for job completion...
# 	sleep 1m
# done

# echo Sorting complete!
