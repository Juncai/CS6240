spark_local () {
	cd Spark
	./run.sh -local
	cd ..
}

spark_emr () {
	cd Spark
	./run.sh -full-emr
	cd ..
}

mr_pd () {
	cd MapReduce
	./run.sh -prepare
	./run.sh -data
	/usr/bin/time -o ../mr_pd_runtime -f "MapReduce_Pseudo-Distributed,%e" -a ./run.sh -pd
	./run.sh -clean
	cd ..
}

mr_emr () {
	cd MapReduce
	./run.sh -full-emr
	cd ..
}

report () {
	Rscript -e "rmarkdown::render('report.Rmd')"
}

if [ "$1" = '-all' ]; then
	mr_pd
	mr_emr
	spark_local
	spark_emr
fi

if [ "$1" = '-spark_local' ]; then
	spark_local
fi

if [ "$1" = '-spark_emr' ]; then
	spark_emr
fi

if [ "$1" = '-mr_pd' ]; then
	mr_pd
fi

if [ "$1" = '-mr_emr' ]; then
	mr_emr
fi

if [ "$1" = '-report' ]; then
	report
fi
