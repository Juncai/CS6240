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

getresult:
	hadoop fs -get output
