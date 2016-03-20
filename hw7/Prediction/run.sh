cd train
./run.sh -prepare
./run.sh -data
./run.sh -pd
cp output/part-r-00000 /tmp/final.rf
cd ..

cd test
gradle jar
./run.sh -data
./run.sh -pd
./run.sh -clean
cd ..
