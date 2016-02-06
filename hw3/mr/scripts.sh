hadoop jar build/libs/Cai_Jun_A2.jar analysis.MRPractice text.txt here
export HADOOP_CLASSPATH=build/classes/main/
hadoop analysis.MRPractice text.txt here
ssh -i ~/.ec2/jon_ec2_key.pem ec2-user@ec2-52-25-202-230.us-west-2.compute.amazonaws.com
