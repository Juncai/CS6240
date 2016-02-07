#!/bin/bash

small_input=$1
large_input=$2
# 298
export MR_INPUT=$1

# prepare for the jobs
echo 'Preparing the environment...'
./run.sh prepare

echo 'Start SingleThread mean with small dataset'
./time.sh './run.sh SingleThread_mean'
echo 'Start SingleThread mean with small dataset'
# ./time.sh './run.sh SingleThread_median'
# ./time.sh './run.sh SingleThread_fastmedian'
# ./time.sh './run.sh Parallel_mean'
# ./time.sh './run.sh Parallel_median'
# ./time.sh './run.sh Parallel_fastmedian'
# # ./time.sh './run.sh EMR_mean'
# # ./time.sh './run.sh EMR_median'
# # ./time.sh './run.sh EMR_fastmedian'
# ./time.sh './run.sh Pseudo-Distributed_mean'
# ./time.sh './run.sh Pseudo-Distributed_median'
# ./time.sh './run.sh Pseudo-Distributed_fastmedian'


# clean
echo 'Cleanup the environment...'
./run.sh clean
