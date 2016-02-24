run() {
	rm -rf out
	sbt "run ${MR_INPUT} out"
}
clean() {
	rm -rf out derby.log metastore_db project target
}


if [ "$1" = '-run' ]; then
	run
fi

if [ "$1" = '-clean' ]; then
	clean
fi


