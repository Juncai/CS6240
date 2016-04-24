#!/bin/bash

taskJar=$1
taskMain=$2
inputPath=$3
outputPath=$4
echo $taskJar
if [ -z $1 ] && [ -z $2 ] && [ -z $3 ] && [ -z $4 ]; then
  echo "Usage: ./my-mapreduce.sh [yourJarFile] [YourMainClass] [YourInputPath] [YourOutputPath]"
  exit 1
fi
echo "uploading test jar to master cluster"
master_ip=$(head -n 1 ./config/ips)

scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $taskJar $EC2_USERNAME@$master_ip:~/test.jar

ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip 'java -cp test.jar $taskMain $inputPath $outputPath >> log.txt 2>&1'


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

