./compile.sh

./start-cluster.sh $1

./sort.sh "dry buld temp" s3://cs6240sp16/climate s3://juncai001/output

./get_logs.sh > /dev/null 2>&1 &

./stop-cluster.sh > /dev/null 2>&1 &
