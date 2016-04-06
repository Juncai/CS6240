#!/bin/bash

# author: Xi Wang

# terminate the instance


while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Terminating node: $line"
    aws ec2 terminate-instances --instance-ids $line
	sleep 3s
done < "id.txt"

#aws ec2 terminate-instances --instance-ids $instance_id
