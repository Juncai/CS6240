#!/bin/bash

run_jobs () {
	echo 'Start SingleThread mean with '${size}' dataset'
	./time.sh './run.sh SingleThread_mean' ${size}
	echo 'Start SingleThread median with '${size}' dataset'
	./time.sh './run.sh SingleThread_median' ${size}
	echo 'Start SingleThread fastmedian with '${size}' dataset'
	./time.sh './run.sh SingleThread_fastmedian' ${size}
	echo 'Start Parallel mean with '${size}' dataset'
	./time.sh './run.sh Parallel_mean' ${size}
	echo 'Start Parallel median with '${size}' dataset'
	./time.sh './run.sh Parallel_median' ${size}
	echo 'Start Parallel fastmedian with '${size}' dataset'
	./time.sh './run.sh Parallel_fastmedian' ${size}
	echo 'Start Pseudo-Distributed mean with '${size}' dataset'
	./time.sh './run.sh Pseudo-Distributed_mean' ${size}
	echo 'Start Pseudo-Distributed median with '${size}' dataset'
	./time.sh './run.sh Pseudo-Distributed_median' ${size}
	echo 'Start Pseudo-Distributed fastmedian with '${size}' dataset'
	./time.sh './run.sh Pseudo-Distributed_fastmedian' ${size}
	echo 'Start EMR mean with '${size}' dataset'
	./time.sh './run.sh EMR_mean' ${size}
	echo 'Start EMR median with '${size}' dataset'
	./time.sh './run.sh EMR_median' ${size}
	echo 'Start EMR fastmedian with '${size}' dataset'
	./time.sh './run.sh EMR_fastmedian' ${size}
}

report () {
	Rscript bars.r
	Rscript -e "rmarkdown::render('report.Rmd')"
}

small_input=$1
large_input=$2
small='small'
large='large'
# 298

# prepare for the jobs
echo 'Preparing the environment...'
./run.sh prepare

# run job with small dataset
export MR_INPUT=${small_input}
size=${small}
# ./run.sh upload_data
# run_jobs

# run job with large dataset
export MR_INPUT=${large_input}
size=${large}
./run.sh upload_data
run_jobs

# generate the report
# report

# clean
echo 'Cleanup the environment...'
./run.sh clean
