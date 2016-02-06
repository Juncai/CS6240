
sequential_mean () {
	java -jar build/libs/analysis.jar analysis.mean.OTPAnalysis -input ${MR_INPUT} -t mean
}

sequential_median () {
	java -jar build/libs/analysis.jar analysis.mean.OTPAnalysis -input ${MR_INPUT} -t median
}

parallel_mean () {
	java -jar build/libs/analysis.jar analysis.mean.OTPAnalysis -input ${MR_INPUT} -t mean -p
}

parallel_median () {
	java -jar build/libs/analysis.jar analysis.mean.OTPAnalysis -input ${MR_INPUT} -t median -p
}

if [ $1 = 'seq_mean' ]; then
	sequential_mean
fi

if [ $1 = 'seq_median' ]; then
	sequential_median
fi

if [ $1 = 'para_mean' ]; then
	parallel_mean
fi

if [ $1 = 'para_median' ]; then
	parallel_median
fi

