#!/bin/bash

host0='54.172.111.165'
host1='54.172.36.157'
port=10001

# node 0
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$host0 'rm -rf ~/cs6240a8'
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$host0 'mkdir ~/cs6240a8'
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$host0:~/cs6240a8/

# node 1
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$host1 'rm -rf ~/cs6240a8'
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$host1 'mkdir ~/cs6240a8'
scp -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH Job.jar $EC2_USERNAME@$host1:~/cs6240a8/

# start jobs
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$host0 "java -jar ~/cs6240a8/Job.jar ${port} ${host1}:${port} > ~/cs6240a8/log 2>&1 &"
ssh -o "StrictHostKeyChecking no" -i $EC2_PRIVATE_KEY_PATH $EC2_USERNAME@$host1 "java -jar ~/cs6240a8/Job.jar ${port} ${host0}:${port} > ~/cs6240a8/log 2>&1 &"
