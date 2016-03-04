index () {
	rm Query/index
	cd Indexing
	make run
	cp out/part-00000 ../Query/index
	cd ..
}

query () {
	cd Query
	# make run
	rm -rf out
	# sbt "run index bowlebr01 out"

	sbt "run index ${pid} out"
	cd ..
}


pid=$2
# echo $pid
if [ "$1" = 'index' ]; then
	index
fi

if [ "$1" = 'query' ]; then
	query
fi


