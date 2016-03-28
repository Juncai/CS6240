#!/bin/bash


host0='127.0.0.1'
host1='127.0.0.1'
port0=10001
port1=10002

# start jobs
java -jar Job.jar ${port0} ${host1}:${port1} > log0 2>&1 &
java -jar Job.jar ${port1} ${host0}:${port0} > log1 2>&1 &
