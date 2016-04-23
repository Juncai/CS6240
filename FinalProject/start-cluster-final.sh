#!/bin/bash

######################Global variables########################
slave_number="2"
# instance_type='m3.2xlarge'
instance_type='m3.medium'

# instances public ip address file
slave_ip_file=config/ips

# instances id file. Used in stop cluster
id_file=id.txt

config_dir="config"
config_path="config/hidoop.conf"

# instances public ip address list
ids[0]='some id'
slave_ips[0]='some ip'
# master_ip='some ip'
#instances status
# master
########################Functions#############################
# function start_master(){
#   echo "---------------------------------------starting master instance------------------------------------"

#   master_id=$(aws ec2 run-instances --image-id $IMAGE_ID \
# 			  --count 1 \
# 			  --instance-type $instance_type \
# 			  --key-name $EC2_KEY_PAIR_NAME \
# 			  --instance-initiated-shutdown-behavior terminate \
# 			  --security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
#   echo $master_id >> $id_file

#   echo "Master " $master_id " started"
#   # need to wait until instance is up
#   echo "Waiting for instance finishing initialization"
#   sleep 2m
# }
function start_slaves(){
  local i="0"
  echo "---------------------------------------Starting slave instance------------------------------------"
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
	  echo "Slave" $slave_id "started"
  	  i=$[$i+1]
  	  sleep 5s
  done

  # need to wait until instance is up
  echo "Waiting for instance finishing initialization"
  sleep 2m
}
function get_ips(){
  echo "------------------------------------------Getting ips----------------------------------------------"
  local i="0"
   ## write out slaves ip addresses
  for id in "${ids[@]}"
  do
	  public_ip=$(aws ec2 describe-instances --instance-ids $id | json Reservations[0].Instances[0].PublicIpAddress)
	  slave_ips[$i]=$public_ip
	  echo $public_ip >> $slave_ip_file
	  i=$[$i+1]
	  echo "slave ip" $public_ip "got"
	  sleep 3s
  done
  sleep 3s
}
function prepare_config(){
  echo "------------------------------------Preparing hidoop.config file-----------------------------------"
  # master_ip=$(aws ec2 describe-instances --instance-ids $master_id | json Reservations[0].Instances[0].PublicIpAddress)
  # echo "Master" $master_ip "got"
  # check if config file exists
  if [ ! -f $config_path ]; then
      echo "create default config file"
      "LOCAL" >> $config_path
      "10001" >> $config_path
      "10002" >> $config_path
  fi

  # check the line numbers in config file
  # line_num=$(wc -l < $config_path)

  # case "$line_num" in
	  # 3)  echo $master_ip >> $config_path
		  # ;;
	  # 4)  sed '$d' $config_path > ./temp
		  # mv ./temp $config_path
		  # echo $master_ip >> $config_path
		  # ;;
	  # *) echo "Invalid config file"
		  # ;;
  # esac
  # sleep 1m
}
function upload_master(){
  echo "---------------------------------Uploading files to master node---------------------------------"
  ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip 'rm -rf ~/*'
  ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$master_ip 'mkdir ~/output ~/.aws ~/config'
  #scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$master_ip:~/
  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH credentials $EC2_USERNAME@$master_ip:~/.aws/
  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $config_dir $EC2_USERNAME@$master_ip:~/
  echo "Done initializing master" $master_ip
  sleep 2s
}
function upload_slaves(){
  i="0"
  echo "--------------------------------Start uploading files to slaves---------------------------------"
  for slave_ip in "${slave_ips[@]}"
  do
  # upload peer and master address list, input file paths, jar file and env.sh
	  ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$slave_ip 'rm -rf ~/*'
	  # ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$slave_ip 'mkdir ~/output ~/.aws ~/config'
	  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$slave_ip:~/
	  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH ~/.aws $EC2_USERNAME@$slave_ip:~/
	  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $config_dir $EC2_USERNAME@$slave_ip:~/
	  i=$[$i+1]
	  echo "Done initializing node:" $slave_ip
  done
}
########################Main##################################
# clean the stale files
rm -rf $slave_ip_file $id_file

# lauch master cluster
start_master

start_slaves
#echo $(aws ec2 describe-instance-status --instance-ids $master_id | json InstanceStatuses[0].InstanceStatus.Status)

prepare_config

get_ips

upload_master

upload_slaves




