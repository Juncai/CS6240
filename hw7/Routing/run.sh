cd train
./run.sh -prepare
./.run.sh -data
./run.sh -pd
cp output/part-r-00000 /tmp/final.rf
cd ..

cd predict
./run.sh -data
./run.sh -pd
cp output/part-r-00000 ${ROUTING_PREDICTION_DIR}/prediction.csv
cd ..

cd query
./run.sh -data
./run.sh -pd
./run.sh -clean
Rcript totalDuration.R output ${ROUTING_VALIDATE_DIR}
cd ..
