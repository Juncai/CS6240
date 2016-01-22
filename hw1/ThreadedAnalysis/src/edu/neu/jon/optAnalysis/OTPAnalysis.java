package edu.neu.jon.optAnalysis;

/**
 * Created by jonca on 1/12/2016.
 *
 * #### My result with input file 323.csv.gz ######
     4082
     435940
     WN 57.79
     HP 65.39
     AS 74.96
     PI 189.67
     CO 203.56
     EA 269.25
     PA (1) 283.09
     US 287.81
     TW 295.68
     AA 304.96
     DL 346.31
     UA 541.65
     NW 53856.85
 * ################### Note about header handling ###################
 * I didn't include the header line as a record, so my K would be one
 * smaller than the reference result (4082 instead of 4083).
 *
 * ##################################################################
 *
 * Input argument: path to gzip file
 * Output:
 *      K (number of corrupted lines)
 *      F (number of sane flights)
 *      C p (where C is the carrier code, p is the mean price of tickets)
 */


public class OTPAnalysis {

    public static void main(String[] args) {
        // naive parser
        if (args.length < 1 || args.length > 2) {
            System.out.println("The usage is $ java SequentialAnalysis GZIIPED_FILE_PATH");
            System.out.println("Or $ java SequentialAnalysis -p -input=DIR");
            return;
        }

        if (args.length == 1) {
            String gzipFilePath = args[0];
            SequentialAnalyzer sa = new SequentialAnalyzer();
            sa.analyze(gzipFilePath);
            sa.printResults();
        }

        if (args.length == 2) {

        }
    }
}