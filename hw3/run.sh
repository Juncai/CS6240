
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

if [ $1 == 'prepare' ]; then
	prepare
fi

if [ $1 == 'clean' ]; then
	clean
fi

if [ $1 == 'SingleThread_mean' ]; then
	run_single_mean
fi

if [ $1 == 'SingleThread_median' ]; then
	run_single_median
fi

if [ $1 == 'SingleThread_fastmedian' ]; then
	run_single_fastmedian
fi

if [ $1 == 'Parallel_mean' ]; then
	run_para_mean
fi

if [ $1 == 'Parallel_median' ]; then
	run_para_median
fi

if [ $1 == 'Parallel_fastmedian' ]; then
	run_para_fastmedian
fi

if [ $1 == 'EMR_mean' ]; then
	run_emr_mean
fi

if [ $1 == 'EMR_median' ]; then
	run_emr_median
fi

if [ $1 == 'EMR_fastmedian' ]; then
	run_emr_fastmedian
fi

if [ $1 == 'Pseudo-Distributed_mean' ]; then
	run_pd_mean
fi

if [ $1 == 'Pseudo-Distributed_median' ]; then
	run_pd_median
fi

if [ $1 == 'Pseudo-Distributed_fastmedian' ]; then
	run_pd_fastmedian
fi

