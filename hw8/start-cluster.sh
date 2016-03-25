#!/bin/bash

# author: Xi Wang

# create a security group
# aws ec2 create-security-group --group-name A8SecurityGroup --description "security group for MR class"

# export EC2_SECURITY_GROUP=A8SecurityGroup

#If you're launching a Windows instance, you must add a rule to my-security-group to allow inbound traffic on TCP port 3389 (RDP).
#If you're launching a Linux instance, you must add a rule to allow inbound traffic on TCP port 22 (SSH).
#Reference: http://docs.aws.amazon.com/cli/latest/userguide/cli-ec2-sg.html#configuring-a-security-group
# aws ec2 authorize-security-group-ingress --group-name A8SecurityGroup --protocol tcp --port 22 --cidr 203.0.113.0/24

# Get input instances number
instance_number=$1

echo $instance_number

# instances public ip address file
ip_file=address.txt

# instances id file
id_file=id.txt


i="0"

# # launch instances
while [ $i -lt $instance_number ]
do
	instance_id=$(aws ec2 run-instances --image-id $IMAGE_ID \
			--count 1 \
			--instance-type t2.micro \
			--key-name $EC2_KEY_PAIR_NAME \
			--instance-initiated-shutdown-behavior terminate \
			--security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
	echo $instance_id >> $id_file

	public_ip=$(aws ec2 describe-instances --instance-ids $instance_id | json Reservations[0].Instances[0].PublicIpAddress)

	echo $public_ip >> $ip_file

	i=$[$i+1]
done



