cd train
# ./run.sh -prepare
./.run.sh -data
./run.sh -pd
cd ..

cd test
./run.sh -data
./run.sh -pd
# ./run.sh -clean
cd ..
