
prepare () {
	cd no_mr
	gradle clean
	gradle jar
	cd ..

	cd mr
	./run.sh -prepare
	cd ..

	cd scala_mr/scala_mr_mean
	make clean
	make jar
	cd ../..

	cd scala_mr/scala_mr_median
	make clean
	make jar
	cd ../..
}

upload_data () {
	cd mr
	./run.sh -data
	cd ..
}

clean () {
	cd mr
	./run.sh -clean
	cd ..
}

run_single_mean () {
	cd no_mr
	./run.sh seq_mean
	cd ..
}

run_single_median () {
	cd no_mr
	./run.sh seq_median
	cd ..
}

run_single_fastmedian () {
	cd no_mr
	./run.sh seq_fastmedian
	cd ..
}

run_para_mean () {
	cd no_mr
	./run.sh para_mean
	cd ..
}

run_para_median () {
	cd no_mr
	./run.sh para_median
	cd ..
}

run_para_fastmedian () {
	cd no_mr
	./run.sh para_fastmedian
	cd ..
}

run_emr_mean () {
	cd mr
	./run.sh -mean -emr
	cd ..
}

run_emr_median () {
	cd mr
	./run.sh -median -emr
	cd ..
}

run_emr_fastmedian () {
	cd mr
	./run.sh -fastmedian -emr
	cd ..
}

run_pd_mean () {
	cd mr
	./run.sh -mean -pd
	cd ..
}

run_pd_median () {
	cd mr
	./run.sh -median -pd
	cd ..
}

run_pd_fastmedian () {
	cd mr
	./run.sh -fastmedian -pd
	cd ..
}

run_scala_mean () {
	cd scala_mr/scala_mr_mean
	make run
	Rscript processOutput.R
	cd ../..
}

run_scala_median () {
	cd scala_mr/scala_mr_median
	make run
	Rscript processOutput.R
	cd ../..
}


if [ $1 == 'prepare' ]; then
	prepare
fi

if [ $1 == 'upload_data' ]; then
	upload_data
fi

if [ $1 == 'clean' ]; then
	clean
fi

if [ $1 == 'SingleThread_m' ]; then
	run_single_mean
fi

if [ $1 == 'SingleThread_med' ]; then
	run_single_median
fi

if [ $1 == 'SingleThread_fmed' ]; then
	run_single_fastmedian
fi

if [ $1 == 'Parallel_m' ]; then
	run_para_mean
fi

if [ $1 == 'Parallel_med' ]; then
	run_para_median
fi

if [ $1 == 'Parallel_fmed' ]; then
	run_para_fastmedian
fi

if [ $1 == 'EMR_m' ]; then
	run_emr_mean
fi

if [ $1 == 'EMR_med' ]; then
	run_emr_median
fi

if [ $1 == 'EMR_fmed' ]; then
	run_emr_fastmedian
fi

if [ $1 == 'Pseudo-Distributed_m' ]; then
	run_pd_mean
fi

if [ $1 == 'Pseudo-Distributed_med' ]; then
	run_pd_median
fi

if [ $1 == 'Pseudo-Distributed_fmed' ]; then
	run_pd_fastmedian
fi

if [ $1 == 'scala_m' ]; then
	run_scala_mean
fi

if [ $1 == 'scala_med' ]; then
	run_scala_median
fi


