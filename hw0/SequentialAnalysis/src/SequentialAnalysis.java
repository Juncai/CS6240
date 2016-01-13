/**
 * Created by jonca on 1/12/2016.
 *
 * Input argument: path to gzip file
 * Output:
 *      K (number of corrupted lines)
 *      F (number of sane flights)
 *      C p (where C is the carrier code, p is the mean price of tickets)
 */
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;


public class SequentialAnalysis {
    private static final String GZIP_FILE = "C:\\Users\\jonca\\Downloads\\one-month\\55.csv.gz";
    private static final String OUTPUT_FILE = "C:\\Users\\jonca\\Downloads\\one-month\\55.csv";

    public static void main(String[] args) {
        SequentialAnalysis sa = new SequentialAnalysis();
        sa.unzipAndAnalyze(SequentialAnalysis.GZIP_FILE);
    }


    public void unzipAndAnalyze(String gzPath) {
        byte[] b = new byte[1024];
        int k = 0;
        int numOfFlights = 0;
        String carrier;

        HashMap<String, Double> priceMap = new HashMap<String, Double>();

        try {
            FileInputStream fis = new FileInputStream(gzPath);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            InputStreamReader isr = new InputStreamReader(gzis);
            BufferedReader br = new BufferedReader(isr);

            String header = br.readLine();
            String line;
            String[] values;

            while ((line = br.readLine()) != null) {
                numOfFlights++;
                // parse the line
//                System.out.println(line);
                values = parseCSVLine(line);
                if (!sanityCheck(values)) {
                    k++;
                } else {

                }
            }

            br.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

//        System.out.println(numOfFlights);
        System.out.println(k);
        System.out.println(numOfFlights - k);
    }

    // Rules
//    CRSArrTime and CRSDepTime should not be zero
//    timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
//    timeZone % 60 should be 0
//    AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
//    Origin, Destination,  CityName, State, StateName should not be empty
//    For flights that not Cancelled:
//    ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
//    if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
//    if ArrDelay < 0 then ArrDelayMinutes should be zero
//    if ArrDelayMinutes >= 15 then ArrDel15 should be false
    public boolean sanityCheck(String[] values) {
        try {
            // Indices
            // # ARR = DEP + 11
            // # not 0
            // CRS_ARR_TIME 40
            // CRS_DEP_TIME 29
            int[] indShouldNotBeZero = {29, 40};
            for (int i : indShouldNotBeZero) {
                if (Integer.parseInt(values[i]) == 0) return false;
            }

            // CRS_ELAPSED_TIME 50
            int timeZone = Integer.parseInt(values[40]) - Integer.parseInt(values[29]) - Integer.parseInt(values[50]);
            int residue = timeZone % 60;
            if (residue != 0) return false;

            // # DEST = ORIGIN + 9
            // # larger than 0
            // ORIGIN_AIRPORT_ID 11
            // ORIGIN_AIRPORT_SEQ_ID 12
            // ORIGIN_CITY_MARKET_ID 13
            // ORIGIN_STATE_FIPS 17
            // ORIGIN_WAC 19
            // DEST_AIRPORT_ID 20 (do we need DIV_AIRPORT_ID?)
            // DEST_AIRPORT_SEQ_ID 21
            // DEST_CITY_MARKET_ID 22
            // DEST_STATE_FIPS 26
            // DEST_WAC 28
            int[] indShouldLargerThanZero = {11, 12, 13, 17, 19, 20, 21, 22, 26, 28};
            for (int i : indShouldLargerThanZero) {
                if (Integer.parseInt(values[i]) <= 0) return false;
            }

            // # not empty
            // ORIGIN 14
            // ORIGIN_CITY_NAME 15
            // ORIGIN_STATE_ABR 16
            // ORIGIN_STATE_NM 18
            int[] indSholdNotBeEmpty = {14, 15, 16, 18};
            for (int i : indSholdNotBeEmpty) {
                if (values[i].equals("")) return false;
            }


            // # for flights not canceled
            // CANCELLED 47
            boolean canceled = (Integer.parseInt(values[47]) != 0);
            // # ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
            // DEP_TIME 30
            // ARR_TIME 41
            // ACTUAL_ELAPSED_TIME 51
            if (!canceled) {
                int timeDiff = Integer.parseInt(values[41]) - Integer.parseInt(values[30])
                        - Integer.parseInt(values[51]) - timeZone;
                if (timeDiff != 0) return false;
            }

            // ARR_DELAY 42
            // ARR_DELAY_NEW 43
            double arrDelay = Double.parseDouble(values[42]);
            double arrDelayNew = Double.parseDouble(values[43]);
            //        if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
            if (arrDelay > 0) {
                if (arrDelay != arrDelayNew) return false;
            }

            //    if ArrDelay < 0 then ArrDelayMinutes???? should be zero
            if (arrDelay < 0) {
                if (arrDelayNew != 0) return false;
            }
            //    if ArrDelayMinutes >= 15 then ArrDel15 should be false
            // ARR_DEL15 44
            boolean arrDel15 = (Double.parseDouble(values[44]) == 0);
            if (arrDelayNew >= 15 && arrDel15) return false;
        } catch (Exception ex) {
//            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public String[] parseCSVLine(String line) {
        ArrayList<String> values = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        boolean inQuote = false;
        char curChar;
        for (int i = 0; i < line.length(); i++) {
            curChar = line.charAt(i);
            if (inQuote) {
                if (curChar == '"') {
                    inQuote = false;
                } else {
                    sb.append(curChar);
                }

            } else {
                if (curChar == '"') {
                    inQuote = true;
                } else if (curChar == ',') {
                    values.add(sb.toString());
                    sb = new StringBuffer();
                } else {
                    sb.append(curChar);
                }
            }
        }
        values.add(sb.toString());

        return values.toArray(new String[1]);
    }
}



