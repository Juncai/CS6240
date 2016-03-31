#!/bin/bash

# author: Xi Wang

# Get input instances number
instance_number=$1

echo $instance_number

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
			--instance-type m3.2xlarge \
			--key-name $EC2_KEY_PAIR_NAME \
			--instance-initiated-shutdown-behavior terminate \
			--security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
	echo $instance_id >> $id_file
	ids[$i]=$instance_id

	i=$[$i+1]
done

# need to wait until instance is up
sleep 3m

i="0"
inputs_prefix='inputs/inputs_'
for id in "${ids[@]}"
do
	public_ip=$(aws ec2 describe-instances --instance-ids $id | json Reservations[0].Instances[0].PublicIpAddress)

	ips[$i]=$public_ip
	echo $public_ip >> $ip_file
	i=$[$i+1]
done

i="0"
for public_ip in "${ips[@]}"
do
# upload peer and master address list, input file paths, jar file and env.sh
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$public_ip 'rm -rf ~/*'
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$public_ip 'mkdir ~/output ~/.aws'
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$public_ip:~/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH credentials $EC2_USERNAME@$public_ip:~/.aws/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $ip_file $EC2_USERNAME@$public_ip:~/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $inputs_prefix$i $EC2_USERNAME@$public_ip:~/

	i=$[$i+1]
	echo "Done initializing node:" $public_ip
done

