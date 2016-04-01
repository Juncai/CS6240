#!/bin/bash

sort_by=$1
input_bucket=$2
output_bucket=$3

# first clean the output dir
aws s3 rm $output_bucket --recursive

# port number to use in the listening socket
port=10001

# ip list of peers
ip_file='address.txt'

# input path list
input_prefix='inputs_'

# ips[0]='some ip'
i="0"

while IFS='' read -r line || [[ -n "$line" ]]; do
	i=$[$i+1]
done < "$ip_file"

heap_size='25g'
# if [ $i = "2" ]; then
# 	heap_size='25g'
# 	# heap_size='2g'
# fi

# counter
i="0"

while IFS='' read -r line || [[ -n "$line" ]]; do
# start the program on each slave
	ssh -i $EC2_PRIVATE_KEY_PATH -n -f $EC2_USERNAME@$line "java -Xmx$heap_size -jar ~/Job.jar $port $ip_file $i $input_bucket $input_prefix$i $output_bucket > ~/log 2>&1 &"
	# ssh -i $EC2_PRIVATE_KEY_PATH -n -f $EC2_USERNAME@$line "rm part-*;nohup java -Xmx25g -jar ~/Job.jar $port $master_port $ip_file $i $INPUT_BUCKET $input_prefix$i junbucket00 > ~/log 2>&1 &"
	i=$[$i+1]
    echo "Node start working: $line"
done < "$ip_file"

# waiting for the all the slave finishing their jobs
finished="0"
while [ $finished != "1" ]; do
	echo Waiting for job completion...
	sleep 1m
	finished=$(java -jar Client.jar $ip_file $port)
	echo $finished
done

# ./shutdown.sh > /dev/null 2>&1 &
