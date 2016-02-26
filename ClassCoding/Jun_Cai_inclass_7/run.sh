index () {
	rm Query/index
	cd Indexing
	make run
	cp out/part-00000 ../Query/index
	cd ..
}

query () {
	cd Query
	make run
	# rm -rf out
	# sbt "run index ${pid} out"
	# sbt "run index ${pid} out"
	cd ..
}


# pid=$1

index
query
