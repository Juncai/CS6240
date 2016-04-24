#!/bin/bash

taskJar=$1
taskMain=$2
inputPath=$3
outputPath=$4

mode=$(head -n 1 ./config/hidoop.conf)
case $mode in
	LOCAL) java -cp $taskJar $taskMain $inputPath $outputPath >> log.txt 2>&1
		  ;;
    EC2)  chmod 755 ./run-ec2.sh
          ./run-ec2.sh $taskJar $taskMain $inputPath $outputPath
   	      ;;
	*) echo "invalid running mode"
          ;;
esac