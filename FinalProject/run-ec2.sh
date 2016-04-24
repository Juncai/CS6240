#!/bin/bash

taskJar=$1
inputPath=$2
outputPath=$3
echo $taskJar
if [ -z $1 ] && [ -z $2 ] && [ -z $3 ]; then
  echo "Usage: ./my-mapreduce.sh [yourJarFile] [YourInputPath] [YourOutputPath]"
  exit 1
fi
echo "uploading job jar to master cluster"
master_ip=$(head -n 1 ./config/ips)

scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $taskJar $EC2_USERNAME@$master_ip:~/Job.jar

ssh -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip "rm log; pkill java; java -jar Job.jar $inputPath $outputPath >> log 2>&1 &"



sleep 10s


echo "uploading job jar to slave clusters"

master_port=$(awk 'NR==2' ./config/hidoop.conf)
slave_port=$(awk 'NR==3' ./config/hidoop.conf)

i="0"
while IFS='' read -r line || [[ -n "$line" ]]; do
	echo slave ip $line
	if [ ${i} -ne "0" ]; then
		ssh -i $EC2_PRIVATE_KEY_PATH -n -f $EC2_USERNAME@$line "pkill java &"
		scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $taskJar $EC2_USERNAME@$line:~/Job.jar
	fi
	i=$[$i+1]
done < './config/ips'

i="0"
while IFS='' read -r line || [[ -n "$line" ]]; do
	echo slave ip $line
	ssh -i $EC2_PRIVATE_KEY_PATH -n -f $EC2_USERNAME@$line "rm -rf slave_log ./part* /tmp/map_* /tmp/reduce*; java -Xmx1024m -cp Job.jar slave.Main $i $slave_port $master_ip $master_port >> slave_log 2>&1 &"
	i=$[$i+1]
done < './config/ips'


# waiting for the all the slave finishing their jobs
echo 'Waiting for job to be done...'
java -jar client/Client.jar $master_ip $master_port
echo 'Get log from master node'
scp -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip:~/log logs/log_$master_ip
# finished="0"
# while [ $finished != "1" ]; do
# 	echo Waiting for job completion...
# 	sleep 1m
# 	finished=$()
# 	echo $finished
# done


