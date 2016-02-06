#!/bin/bash

large_input=$1
small_input=$2

export MR_INPUT=$1
./time.sh './run.sh SingleThread_mean'
./time.sh './run.sh SingleThread_median'
./time.sh './run.sh Parallel_mean'
./time.sh './run.sh Parallel_median'
# ./time.sh './run.sh EMR_mean'
# ./time.sh './run.sh EMR_median'
./time.sh './run.sh Pseudo-Distributed_mean'
./time.sh './run.sh Pseudo-Distributed_median'

