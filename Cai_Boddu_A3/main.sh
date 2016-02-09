#!/bin/bash

run_jobs () {
	echo 'Start SingleThread m with '${size}' dataset'
	./time.sh './run.sh SingleThread_m' ${size}
	echo 'Start SingleThread med with '${size}' dataset'
	./time.sh './run.sh SingleThread_med' ${size}
	echo 'Start SingleThread fmed with '${size}' dataset'
	./time.sh './run.sh SingleThread_fmed' ${size}
	echo 'Start Parallel m with '${size}' dataset'
	./time.sh './run.sh Parallel_m' ${size}
	echo 'Start Parallel med with '${size}' dataset'
	./time.sh './run.sh Parallel_med' ${size}
	echo 'Start Parallel fmed with '${size}' dataset'
	./time.sh './run.sh Parallel_fmed' ${size}
	echo 'Start Pseudo-Distributed m with '${size}' dataset'
	./time.sh './run.sh Pseudo-Distributed_m' ${size}
	echo 'Start Pseudo-Distributed med with '${size}' dataset'
	./time.sh './run.sh Pseudo-Distributed_med' ${size}
	echo 'Start Pseudo-Distributed fmed with '${size}' dataset'
	./time.sh './run.sh Pseudo-Distributed_fmed' ${size}
	echo 'Start EMR m with '${size}' dataset'
	./time.sh './run.sh EMR_m' ${size}
	echo 'Start EMR med with '${size}' dataset'
	./time.sh './run.sh EMR_med' ${size}
	echo 'Start EMR fmed with '${size}' dataset'
	./time.sh './run.sh EMR_fmed' ${size}
	# echo 'Start Scala m with '${size}' dataset'
	# ./time.sh './run.sh scala_m' ${size}
	# echo 'Start Scala med with '${size}' dataset'
	# ./time.sh './run.sh scala_med' ${size}
}

report () {
	Rscript bars.r
	Rscript -e "rmarkdown::render('report.Rmd')"
}

small_input=$1
large_input=$2
small='s'
large='l'

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
