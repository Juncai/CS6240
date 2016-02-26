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

neha:
	hadoop fs -mkdir -p /user/neha
	hadoop fs -mkdir -p /user/neha/input

pseudo:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar  HW05.java
	jar cvf job.jar *.class
	export HADOOP_CLASSPATH=.:`hadoop classpath`
	hadoop dfs -copyFromLocal /home/neha/Downloads/all /user/neha/input	
	hadoop jar job.jar HW05 /user/neha/input/all output
	hadoop fs -get output

