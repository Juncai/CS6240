class Test {
	public static void main(String[] args) throws Exception {
		String comm = "Rscript /tmp/rf.R /tmp/OTP_prediction_training_5820e70b-0376-4486-8339-24c9d07f1e5b.csv /tmp/OTP_prediction_c0ddf90a-7c52-44dc-ac4d-34fe5fadbc37.rf";
		String fName = "/tmp/OTP_prediction_training_dfe096fb-fe32-4d78-8371-19eb3654c587.csv";
		String path = "/tmp/OTP_prediction_test1.rf";
		String[] argArray = new String[4];
		argArray[0] = "Rscript";
		argArray[1] = "/tmp/rf.R";
		argArray[2] = fName;
		argArray[3] = path;
        // Process p = Runtime.getRuntime().exec("Rscript /tmp/rf.R " + fName + " " + path);
        Process p = Runtime.getRuntime().exec(comm);
        // Process p = Runtime.getRuntime().exec(argArray, null);
		p.waitFor();
	}
}
