#!/bin/bash

emrinput=0

sanityCheck () {
	: "${MR_INPUT:?Please set the input data path in MR_INPUT!}"
}

clean () {
	rm -rf output
	rm -rf log
	rm -rf /tmp/hadoop-${USER}
}

rmpwd () {
	export AWS_ACCESS_KEY=
	export AWS_SECRET_KEY=
	export AWS_DEFAULT_REGION=
}

cmp () {
	# building the code
	gradle clean
	gradle jar
}

pd () {
	# set hadoop configure file path
	export HADOOP_CONF_DIR=${PWD}/.hadoop/
	export HADOOP_LOG_DIR=${PWD}/log/
	export HADOOP_CLASSPATH=

	# format
	hdfs namenode -format

	# hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

	# create user folder in hdfs then upload the input files
	hadoop fs -mkdir -p /user/${USER}
	hadoop fs -put ${MR_INPUT} input

	# run the job
	hadoop jar build/libs/ClusterAnalysis.jar ClusterAnalysis input output

	# get the output
	hadoop fs -get output output

	# hstop:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh
}

emr () {
	bucketname=juncai001
	# set aws credentials
	export AWS_ACCESS_KEY=AKIAI6WT62DBECZIFLHQ
	export AWS_SECRET_KEY=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
	export AWS_DEFAULT_REGION=us-west-2

	# create s3 bucket
	# aws s3 rb s3://${bucketname} --force
	# aws s3 mb s3://${bucketname}
	# upload the data files
	if [ ${emrinput} = 1 ]; then
		aws s3 sync ${MR_INPUT} s3://${bucketname}/input
		# aws s3 sync log s3://${bucketname}/log
	fi
		# upload jar file
	aws s3 cp build/libs/ClusterAnalysis.jar s3://${bucketname}/ClusterAnalysis.jar

	# configure EMR
	loguri=s3n://${bucketname}/log/
	cid=$(aws emr create-cluster --applications Name=Hadoop \
		--ec2-attributes '{"KeyName":"jon_ec2_key","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-49105b10","EmrManagedSlaveSecurityGroup":"sg-1011ee77","EmrManagedMasterSecurityGroup":"sg-1111ee76"}'\
		--service-role EMR_DefaultRole \
		--enable-debugging \
		--release-label emr-4.3.0 \
		--log-uri ${loguri} \
		--steps '[{"Args":["ClusterAnalysis","s3://juncai001/input","s3://juncai001/output"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://juncai001/ClusterAnalysis.jar","Properties":"","Name":"ClusterAnalysis"}]' \
		--name 'Jun MR cluster' \
		--instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m1.medium","Name":"Master Instance Group"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m1.medium","Name":"Core Instance Group"}]' \
		--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
		--auto-terminate \
		--region us-west-2 | grep -oh 'j-[0-9A-Z]\{13\}')
	
	# waiting for job to complete
	tmstate='            "State": "TERMINATED",'
	output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
	while [ "$output" != "$tmstate" ]; do
		output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
		echo Waiting for job completion...
		sleep 1m
	done

	# get the output
	aws s3 sync s3://${bucketname}/output output --delete
	# clean the files
	# aws s3 rm s3://juncai001/input --recursive
	aws s3 rm s3://${bucketname}/output --recursive
}

report () {
	Rscript -e "rmarkdown::render('report.Rmd')"
}

if [ "$2" = '-EMRInput' ]; then
	sanityCheck
	emrinput=1
fi

clean
cmp

if [ $1 = '-pd' ]; then
	pd
fi

if [ $1 = '-emr' ]; then
	emr
fi

report

rmpwd
