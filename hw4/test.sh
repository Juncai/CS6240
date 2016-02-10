rm -rf output
gradle build
hadoop jar build/libs/LinearRegressionFit.jar analysis.LinearRegressionFit $MR_INPUT output
