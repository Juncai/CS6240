#!/bin/bash
# -oh 'j-[0-9A-Z]\{13\}'
dd=$(echo \
		something)
echo "$dd"
cid='j-15ASE08MBMJ7W'
tmstate='            "State": "TERMINATED",'
output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
while [ "$output" != "$tmstate" ]; do
	output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
	echo Waiting for job completion...
	sleep 1m
done
# while read -r line; do
# 	if [ "$line" = '' ]; then
# 		echo waiting for job completion
# 		sleep -m 1
# 	else
# 		echo I read "$line"
# 	fi
# done <<< "$output"
