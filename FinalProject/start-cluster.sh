#!/bin/bash

# author: Xi Wang

# Get input instances number
instance_number=$1
if [ $instance_number != "2" ] && [ $instance_number != "8" ]; then
	echo "The valid instance numbers are 2 and 8"
	exit 1
fi

# if [ $instance_number = "2" ]; then
instance_type='m3.2xlarge'
	# instance_type='m3.medium'
# else
	# instance_type='m3.xlarge'
# fi


echo starting $instance_number $instance_type instances

# instances public ip address file
ip_file=address.txt

# instances public ip address list
ids[0]='some id'
ips[0]='some ip'

# instances id file
id_file=id.txt

# clean the stale files
rm $ip_file $id_file

i="0"

# # launch instances
while [ $i -lt $instance_number ]
do
	instance_id=$(aws ec2 run-instances --image-id $IMAGE_ID \
			--count 1 \
			--instance-type $instance_type \
			--key-name $EC2_KEY_PAIR_NAME \
			--instance-initiated-shutdown-behavior terminate \
			--security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
	echo $instance_id >> $id_file
	ids[$i]=$instance_id

	i=$[$i+1]
	sleep 5s
done

# need to wait until instance is up
sleep 2m

i="0"
inputs_prefix='inputs/inputs_'
for id in "${ids[@]}"
do
	public_ip=$(aws ec2 describe-instances --instance-ids $id | json Reservations[0].Instances[0].PublicIpAddress)

	ips[$i]=$public_ip
	echo $public_ip >> $ip_file
	i=$[$i+1]
	sleep 3s
done

# need to wait until instance is up
sleep 1m

i="0"
for public_ip in "${ips[@]}"
do
# upload peer and master address list, input file paths, jar file and env.sh
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$public_ip 'rm -rf ~/*'
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$public_ip 'mkdir ~/output ~/.aws'
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$public_ip:~/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH credentials $EC2_USERNAME@$public_ip:~/.aws/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $ip_file $EC2_USERNAME@$public_ip:~/
	# scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $inputs_prefix$i $EC2_USERNAME@$public_ip:~/

	i=$[$i+1]
	echo "Done initializing node:" $public_ip
done

