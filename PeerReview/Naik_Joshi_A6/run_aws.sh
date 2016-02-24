#!/bin/bash

# authors Rohan Joshi, Vedant Naik

# The flow of the program ->
# 1. fire the EMR cluster 
# 2. get the job number or the cluster id in a variable
# 3. Use that variable to keep polling the cluster status
# 4. Once the status changes to "TERMINATED", we end the loop and download the output/* into a local folder

# based on the os, please uncomment one of the two lines to install jq, the tool that greps JSON data that is
# returned by the polling (may require sudo)
# please enter the password whenever prompted if the install commands are executed

# for linux: 
# sudo apt-get install jq

# for OS X
# brew install jq

# args for the java program are::  
# args[0] : path to downloaded folder

downloadpath=$1
echo $downloadpath

bucketname=$2
echo $bucketname

aws s3 cp all/ s3://$bucketname/input/all/ --recursive
aws s3 cp MissedConnections.jar s3://$bucketname
aws s3 ls s3://$bucketname
echo $PATH
#cluster_id="j-1L5ZOXYTPB54G"
#args1 are -mean, -median or -fastMedian
#args1=$1
#echo $args1
#$1="/Users/rohanjoshi/Documents/output_aws4/"
#eval job_id=`aws emr create-cluster --name "CLI-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Name="ClusterAnalysis",ActionOnFailure=CONTINUE,Jar=s3://clusteranalysis/ClusterAnalysis.jar,MainClass=ClusterAnalysis,Args=[s3://clusteranalysis/input/all,s3://clusteranalysis/output/] --auto-terminate --log-uri s3://clusteranalysis/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`
eval job_id=`aws emr create-cluster --name "LIN-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=m1.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m1.large --steps Type=CUSTOM_JAR,Name="MissedConnections",ActionOnFailure=CONTINUE,Jar=s3://$bucketname/MissedConnections.jar,MainClass=MissedConnections,Args=[-emr,s3://$bucketname/input/all,s3://$bucketname/output/] --auto-terminate --log-uri s3://$bucketname/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`

echo $job_id
echo "deleting the log directory in the S3 bucket"
aws s3 rm s3://$bucketname/log --recursive

echo "deleting the output directory in the S3 bucket"
aws s3 rm s3://$bucketname/output --recursive

status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
echo $status
flag="False"
while [ $flag == "False" ]
do
	sleep 5
	status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
	echo $status
	if [ $status = '"TERMINATED"' ]; then
		flag="True"
	else
		flag="False"
	fi
done
finpath='outputAws'
aws s3 sync s3://$bucketname/output $finpath

# uncomment this line if you want to locally download the logs
#aws s3 sync s3://linearreg/log/$job_id/steps/ /Users/rohanjoshi/Documents/output_aws/log/

echo "The job has terminated and all the output files downloaded..."
echo "The folder where the part files are located is the downloadpath variable that you specified in the script.cfg file"
#cat /Users/rohanjoshi/Documents/output_aws/* > output_aws.txt
#cd /Users/rohanjoshi/Documents/workspace_exp/Read/src

#javac Read.java
#java Read /Users/rohanjoshi/Documents/output_aws/logs $args1
echo "completed if uptil here"
#echo "Output will contain time taken : -- and located in /Users/rohanjoshi/Documents/output_time$args1.txt"
