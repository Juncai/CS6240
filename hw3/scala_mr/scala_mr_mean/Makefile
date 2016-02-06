#!/bin/bash
# Author: Nat Tuck

jar:
	sbt package
	cp target/scala-*/otpanalysis_*.jar OTPAnalysis.jar

run: jar
	rm -rf output
	# sbt run ~/Downloads/part output
	sbt run 

run_only:
	rm -rf output
	sbt run

hadoop: jar
	rm -rf output
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar OTPAnalysis.jar

clean:
	rm -rf project target *.jar output
