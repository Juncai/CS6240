#!/bin/bash

######################Global variables########################
instance_number=$1
instance_type='t2.small'

# instances public ip address file
ip_file=config/ips

# instances id file. Used in stop cluster
id_file=id.txt

config_dir="config"
config_path="config/hidoop.conf"

# instances public ip address list
ids[0]='some id'
ips[0]='some ip'
########################Functions#############################
if [ -z $1 ]; then
  echo "Usage: ./start-cluster-final.sh [number of instances]"
  exit 1
fi

function start_instance(){
  local i="0"
  echo "---------------------------------------Starting instance------------------------------------"
  # # launch all instances
  while [ $i -lt "$instance_number" ]
  do
	  id=$(aws ec2 run-instances --image-id $IMAGE_ID \
			  --count 1 \
			  --instance-type $instance_type \
			  --key-name $EC2_KEY_PAIR_NAME \
			  --instance-initiated-shutdown-behavior terminate \
			  --security-groups $EC2_SECURITY_GROUP | json Instances[0].InstanceId)
	  echo $id >> $id_file
	  ids[$i]=$id
	  echo "Instance $id started"
  	  i=$[$i+1]
  	  sleep 5s
  done

  # need to wait until instance is up
  echo "Waiting for instance finishing initialization"
  sleep 5m
}
function get_ips(){
  echo "------------------------------------------Getting ips----------------------------------------------"
  local i="0"
   ## write out ip addresses
  for id in "${ids[@]}"
  do
	  public_ip=$(aws ec2 describe-instances --instance-ids $id | json Reservations[0].Instances[0].PublicIpAddress)
	  ips[$i]=$public_ip
	  echo $public_ip >> $ip_file
	  i=$[$i+1]
	  echo "IP: " $public_ip "got"
	  sleep 3s
  done
  sleep 3s
}
function prepare_config(){
  echo "------------------------------------Preparing hidoop.config file-----------------------------------"
  # check if config file exists
  if [ ! -f $config_path ]; then
      echo "create default config file"
      "EC2" >> $config_path
      "10001" >> $config_path
      "10002" >> $config_path
  fi
}

function upload_files(){
  i="0"
  echo "--------------------------------Start uploading files to Instances---------------------------------"
  for ip in "${ips[@]}"
  do
  # upload peer and master address list, input file paths, jar file and env.sh
	  ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$ip 'rm -rf ~/*'
	  # ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$ip 'mkdir ~/output'
	  # scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$ip:~/
	  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH -r ~/.aws $EC2_USERNAME@$ip:~/
	  scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH -r $config_dir $EC2_USERNAME@$ip:~/
	  i=$[$i+1]
	  echo "Done initializing node:" $ip
  done
}
########################Main##################################
# clean the stale files
rm -rf $ip_file $id_file

start_instance

prepare_config
get_ips

chmod +x ./compile-final.sh

./compile-final.sh

upload_files




