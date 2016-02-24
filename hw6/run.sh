#!/bin/bash

emrinput=0

sanityCheck () {
	: "${MR_INPUT:?Please set the input data path in MR_INPUT!}"
}

clean () {
	rm -rf output project target
	rm -rf log
	sbt clean clean-cache clean-lib clean-plugins
}

cmp () {
	# building the code
	sbt assembly
}

upload_data_emr() {
	# upload the data files
	aws s3 rm s3://${BUCKET_NAME}/input --recursive
	aws s3 sync ${MR_INPUT} s3://${BUCKET_NAME}/input
}

local_mode () {
	rm -rf output
	sbt "run ${MR_INPUT} output local"
}

emr () {
	# set aws credentials
	export AWS_DEFAULT_REGION=us-west-2

	# create s3 bucket
	# aws s3 rb s3://${BUCKET_NAME} --force
	# aws s3 mb s3://${BUCKET_NAME}
	# clean the files
	aws s3 rm s3://${BUCKET_NAME}/output --recursive
	aws s3 rm s3://${BUCKET_NAME}/Job.jar

	# upload jar file
	aws s3 cp target/scala-2.10/Job.jar s3://${BUCKET_NAME}/Job.jar

	# configure EMR
	loguri=s3n://${BUCKET_NAME}/log/
		
	cid=$(aws emr create-cluster --applications Name=Spark \
		--ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole"}' \
		--service-role EMR_DefaultRole \
		--enable-debugging \
		--release-label emr-4.3.0 \
		--log-uri ${loguri} \
		--steps '[{"Args":["s3://'${BUCKET_NAME}'/input","s3://'${BUCKET_NAME}'/output"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://'${BUCKET_NAME}'/Job.jar","Properties":"","Name":"MissedConnectionsAnalysis"}]' \
		--name 'Jun MR cluster' \
		--instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m1.medium","Name":"Master Instance Group"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m1.medium","Name":"Core Instance Group"}]' \
		--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
		--auto-terminate \
		--region us-west-2 | grep -oh 'j-[0-9A-Z][0-9A-Z]*')

	# waiting for job to complete
	tmstate='            "State": "TERMINATED",'
	output=$(aws emr describe-cluster --output json --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
	while [ "$output" != "$tmstate" ]; do
		output=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{12\}"State": "TERMINATED",')
		echo Waiting for job completion...
		sleep 1m
	done

	# get the output
	aws s3 sync s3://${BUCKET_NAME}/output output --delete
}

get_output_emr () {
	aws s3 sync s3://${BUCKET_NAME}/output output --delete
}

report () {
	Rscript -e "rmarkdown::render('report.Rmd')"
}

process_output () {
	Rscript processOutput.R
}

if [ $1 = '-clean' ]; then
	clean
	# stop_server
fi

if [ $1 = '-prepare' ]; then
	clean
	cmp
	start_server
fi

if [ $1 = '-data' ]; then
	upload_data_pd
	# upload_data_emr
fi


input_path='input'

if [ "$1" = '-local' ]; then
	# clean
	local_mode
	# process_output
fi

if [ "$1" = '-emr' ]; then
	# clean
	emr
	# process_output
fi
if [ "$1" = '-full-local' ]; then
	clean
	local_mode
# process result by R
#	process_output
	report
fi

if [ "$1" = '-full-emr' ]; then
	clean
	cmp
	upload_data_emr
	emr
# process result by R
	# process_output
	report
fi

if [ "$1" = '-continue' ]; then
	get_output_emr
# process result by R
	# process_output
	report
fi


# report
