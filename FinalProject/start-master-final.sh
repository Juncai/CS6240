#!/bin/bash

instance_type='m3.2xlarge'

# instances id file. Used in stop cluster
id_file=id.txt

# master public ip address file
master_ip_file = masterIP.txt

# clean the ip files
rm $master_ip_file 

# write master cluster id into id.txt
master_id=$(aws ec2 run-instances --image-id $IMAGE_ID \
			--count 1 \
			--instance-type $instance_type \
			--key-name $EC2_KEY_PAIR_NAME \
			--instance-initiated-shutdown-behavior terminate \
			--security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
echo $master_id >> $id_file


# need to wait until instance is up
sleep 2m

# write out master public ip address into masterIP.txt
master_ip=$(aws ec2 describe-instances --instance-ids $master_id | json Reservations[0].Instances[0].PublicIpAddress)

# check if config file exists
if [ ! -f $config_path ]; then
    echo "create default config file"
    "LOCAL" >> $config_path
    "10001" >> $config_path
    "10002" >> $config_path
fi

# check the line numbers in config file
line_num=$(wc -l < $config_path)

case "$line_num" in
	3)  $master_ip >> $config_path
		;;
	4)  sed '$d' $config_path > ./temp
		mv ./temp $config_path 
		$master_ip >> $config_path
		;;
	*) echo "Invalid config file"
		;;
esac

ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip 'rm -rf ~/*'
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip 'mkdir ~/output ~/.aws ~/config'
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$master_ip:~/
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH credentials $EC2_USERNAME@$master_ip:~/.aws/
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $ip_file $EC2_USERNAME@$master_ip:~/
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $config_path $EC2_USERNAME@$master_ip:~/config
