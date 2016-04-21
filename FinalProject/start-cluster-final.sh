#!/bin/bash


slave_number="2"
instance_type='m3.2xlarge'

# instances public ip address file
slave_ip_file=address.txt

# instances id file. Used in stop cluster
id_file=id.txt

# master public ip address file
master_ip_file = masterIP.txt

config_path="./Hidoop/config/hidoop.conf"

# instances public ip address list
ids[0]='some id'
slave_ips[0]='some ip'

# clean the stale files
rm $ip_file $id_file

# lauch master cluster
./start-master-final.sh



i="0"

# # launch all instances
while [ $i -lt $slave_number ]
do
	slave_id=$(aws ec2 run-instances --image-id $IMAGE_ID \
			--count 1 \
			--instance-type $instance_type \
			--key-name $EC2_KEY_PAIR_NAME \
			--instance-initiated-shutdown-behavior terminate \
			--security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
	echo $slave_id >> $id_file
	ids[$i]=$slave_id

	i=$[$i+1]
	sleep 5s
done

# need to wait until instance is up
sleep 2m

## write out slaves ip addresses
for id in "${ids[@]}"
do
	public_ip=$(aws ec2 describe-instances --instance-ids $id | json Reservations[0].Instances[0].PublicIpAddress)

	slave_ips[$i]=$public_ip
	echo $public_ip >> $slave_ip_file
	i=$[$i+1]
	sleep 3s
done

# need to wait until instance is up
sleep 1m

i="0"
for slave_ip in "${slave_ips[@]}"
do
# upload peer and master address list, input file paths, jar file and env.sh
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$slave_ip 'rm -rf ~/*'
	ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$slave_ip 'mkdir ~/output ~/.aws ~/config'
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$slave_ip:~/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH credentials $EC2_USERNAME@$slave_ip:~/.aws/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $ip_file $EC2_USERNAME@$slave_ip:~/
	scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $config_path $EC2_USERNAME@$slave_ip:~/config

	i=$[$i+1]
	echo "Done initializing node:" $slave_ip
done

