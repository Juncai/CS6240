#!/bin/bash

emrinput=0

sanityCheck () {
	: "${MR_INPUT:?Please set the input data path in MR_INPUT!}"
}

clean () {
	rm -rf output
	rm -rf log
}

rmpwd () {
	export AWS_DEFAULT_REGION=
}

cmp () {
	# building the code
	gradle clean
	gradle jar
}

start_server () {
	# clean the tmp folder
	rm -rf /tmp/hadoop-${USER}
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
	# hadoop fs -put ${MR_INPUT} input

	# remove the previous output from server if any
	hadoop fs -rm -r output
}

upload_data_pd () {
	hadoop fs -rm -r input
	hadoop fs -rm -r tmp
	hadoop fs -put ${MR_INPUT} input
	hadoop fs -put tmp tmp 
}

upload_data_emr() {
	# upload the data files
	aws s3 rm s3://${bucketname}/input --recursive
	aws s3 rm s3://${bucketname}/tmp --recursive
	aws s3 sync ${MR_INPUT} s3://${bucketname}/input
	aws s3 sync tmp s3://${bucketname}/tmp
}

stop_server () {
	# hstop:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh
}

pd () {
	# remove the previous output from server if any
	hadoop fs -rm -r output

	# run the job
	hadoop jar build/libs/LinearRegressionFit.jar analysis.LinearRegressionFit input output tmp/stats
	# hadoop jar build/libs/ClusterAnalysis.jar analysis.ClusterAnalysis input output ${task} 2>>hd_log

	# get the output
	hadoop fs -get output output

	# use R to process the output

}

emr () {
	# set aws credentials
	export AWS_DEFAULT_REGION=us-west-2

	# create s3 bucket
	# aws s3 rb s3://${bucketname} --force
	# aws s3 mb s3://${bucketname}
	# clean the files
	aws s3 rm s3://${bucketname}/output --recursive
	aws s3 rm s3://${bucketname}/Job.jar

	# upload jar file
	aws s3 cp build/libs/LinearRegressionFit.jar s3://${bucketname}/Job.jar

	# configure EMR
	loguri=s3n://${bucketname}/log/
		
	cid=$(aws emr create-cluster --applications Name=Hadoop \
		--ec2-attributes '{"KeyName":"jon_ec2_key","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-49105b10","EmrManagedSlaveSecurityGroup":"sg-1011ee77","EmrManagedMasterSecurityGroup":"sg-1111ee76"}'\
		--service-role EMR_DefaultRole \
		--enable-debugging \
		--release-label emr-4.3.0 \
		--log-uri ${loguri} \
		--steps '[{"Args":["analysis.LinearRegressionFit","s3://'${bucketname}'/input","s3://'${bucketname}'/output", "s3://'${bucketname}'/tmp/stats"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://'${bucketname}'/Job.jar","Properties":"","Name":"LinearRegressionFit"}]' \
		--name 'Jun MR cluster' \
		--instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m1.medium","Name":"Master Instance Group"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m1.medium","Name":"Core Instance Group"}]' \
		--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
		--auto-terminate \
		--region us-west-2 | grep -oh 'j-[0-9A-Z][0-9A-Z]*')

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
}

get_stats () {
	rm -rf tmp
	mkdir tmp
	Rscript mean_std.R
}

report () {
	Rscript -e "rmarkdown::render('report.Rmd')"
}

process_output () {
	Rscript processOutput.R
}

bucketname=juncai001

if [ $1 = '-clean' ]; then
	clean
	rmpwd
	stop_server
fi

if [ $1 = '-prepare' ]; then
	clean
	cmp
	get_stats
	start_server
fi

if [ $1 = '-data' ]; then
	upload_data_pd
	upload_data_emr
fi


input_path='input'

if [ "$1" = '-pd' ]; then
	clean
	pd
	# process_output
fi

if [ "$1" = '-emr' ]; then
	clean
	emr
	# process_output
fi
if [ "$1" = '-full-pd' ]; then
	clean
	cmp
	get_stats
	start_server
	upload_data_pd
	pd
	stop_server
# process result by R
	process_output
fi

if [ "$1" = '-full-emr' ]; then
	clean
	cmp
	get_stats
	upload_data_emr
	emr
# process result by R
	process_output
fi

# report
