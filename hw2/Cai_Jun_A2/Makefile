envvar:
	export AWS_CREDENTIAL_PROFILES_FILE=.aws/credentials

jar:
	gradle clean
	gradle jar

format:
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

cpath:
	export HADOOP_CLASSPATH=build/classes/main

prepare:
	rm -rf output
	hadoop fs -mkdir -p /user/jon
	hadoop fs -put /home/jon/Downloads/all input
	# hadoop jar build/libs/ClusterAnalysis.jar FileDecompressor input/*
	# hadoop fs -rm input/*.gz
	
runjob:
	# hadoop jar build/libs/ClusterAnalysis.jar ClusterAnalysis input/* output
	hadoop jar build/libs/ClusterAnalysis.jar ClusterAnalysis input output
runjobEMR:
	aws emr create-cluster --applications Name=Hadoop --ec2-attributes '{"KeyName":"jon_ec2_key","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-49105b10","EmrManagedSlaveSecurityGroup":"sg-1011ee77","EmrManagedMasterSecurityGroup":"sg-1111ee76"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://junbucket00/log/' --steps '[{"Args":["MRPractice","s3://junbucket00/input/text.txt","s3://junbucket00/output"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://junbucket00/MRPractice.jar","Properties":"","Name":"MRPractice JAR"}]' --name 'Jun's MR cluster' --instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m1.medium","Name":"Core Instance Group"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m1.medium","Name":"Master Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --auto-terminate --region us-west-2
getresult:
	hadoop fs -get output
