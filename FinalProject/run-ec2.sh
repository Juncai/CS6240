#!/bin/bash

taskJar=$1
inputPath=$2
outputPath=$3

echo "uploading test jar to master cluster"
master_ip=$(head -n 1 ./config/ips)
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KE $taskJar $EC2_USERNAME@$master_ip:~/

ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip 'java -jar $taskJar $inputPath $outputPath'


sleep 1m


echo "uploading test jar to slave clusters"

master_port=$(awk 'NR==2' ./config/hidoop.conf)
slave_port=$(awk 'NR==3' ./config/hidoop.conf)

i="0"
while IFS='' read -r line || [[ -n "$line" ]]; do
	echo slave ip $line
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$line 'java -Xmx1024m -cp Job.jar slave.Main $i $slave_port $master_ip $master_port'
	rm -rf /tmp/map_* /tmp/reduce*
	i=$[$i+1]
done < "./config/ips"

