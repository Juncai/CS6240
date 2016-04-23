#!/bin/bash

taskJar=$1
inputPath=$2
outputPath=$3

mode=$(head -n 1 ./config/hidoop.conf)
case mode in
	LOCAL) java -jar $taskJar $inputPath $outputPath
		  ;;
    EC2) ./run-ec2.sh $taskJar $inputPath $outputPath
   	      ;;
	*) echo "invalid running mode"
          ;;
esac