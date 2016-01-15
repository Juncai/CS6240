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

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class SequentialAnalysis {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("The usage is $ java SequentialAnalysis GZIIPED_FILE_PATH");
            return;
        }

        String gzipFilePath = args[0];
        SequentialAnalysis sa = new SequentialAnalysis();
        sa.unzipAndAnalyze(gzipFilePath);
    }

    /**
     * Unzip the gzipped file, then parse it and calculate the mean
     * ticket price for each carrier. Print out the number of corrupted
     * lines and sane flights, and a list of mean ticket price for each
     * carrier
     * @param gzPath path to the GZIP file
     */
    public void unzipAndAnalyze(String gzPath) {
        int[] stats;
        HashMap<String, List<Double>> priceMap = new HashMap<>();

        try {
            FileInputStream fis = new FileInputStream(gzPath);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            InputStreamReader isr = new InputStreamReader(gzis);

            stats = getPriceMapWithBufferedReader(isr, priceMap);

        } catch (IOException ex) {
            //  ex.printStackTrace();
            return;
        }

        // generate price list then sort it
        List<String[]> priceList = getSortedPrices(priceMap);

        System.out.println(stats[0]);
        System.out.println(stats[1]);
        printPriceList(priceList);
    }

    /**
     * Get flight count and sum of ticket price for each carrier from a reader
     * @param reader reader contains the unzipped stream
     * @param priceMap key is the airline carrier ID, value is a list of two floats where
     *                 the first one is the number of flights, the second one is the sum
     *                 of all the flight ticket price.
     * @return integer array contains number of corrupted records and sane flights
     * @throws IOException
     */
    public int[] getPriceMapWithBufferedReader(Reader reader, HashMap<String, List<Double>> priceMap) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        int numOfFlights = 0;
        int k = 0;
        String carrier;
        Double price;

        br.readLine();  // get rid of the header
        String line;
        String[] values;
        while ((line = br.readLine()) != null) {
            numOfFlights++;
            values = parseCSVLine(line);
            if (!sanityCheck(values)) {
                k++;
            } else {
                // UNIQUE_CARRIER 6
                // AVG_TICKET_PRICE 109
                carrier = values[6];
                price = Double.parseDouble(values[109]);
                if (!priceMap.containsKey(carrier)) {
                    priceMap.put(carrier, Arrays.asList(1.0, price));
                } else {
                    List<Double> curList = priceMap.get(carrier);
                    curList.set(0, curList.get(0) + 1);
                    curList.set(1, curList.get(1) + price);
                }
            }
        }

        br.close();

        return new int[]{k, numOfFlights - k};
    }

    /**
     * Parse a line in CSV format
     * @param line a string in CSV format
     * @return list of values as a string array
     */
    public String[] parseCSVLine(String line) {
        ArrayList<String> values = new ArrayList<>();
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
        values.add(sb.toString());  // last field

        return values.toArray(new String[1]);
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
    /**
     * Check the number of fields in the record and the logic between some fields
     * @param values a String array contains the value of each field in a record
     * @return true if the input is a valid record, false otherwise
     */
    public boolean sanityCheck(String[] values) {
        if (values.length != 110) return false;
        try {
            // Indices
            // # ARR = DEP + 11
            // # not 0
            // CRS_ARR_TIME 40
            // CRS_DEP_TIME 29
            int[] indShouldNotBeZero = {29, 40};
            for (int i : indShouldNotBeZero) {
                if (Double.parseDouble(values[i]) == 0) return false;
            }

            // CRS_ELAPSED_TIME 50
            double timeZone = getMinDiff(values[40], values[29]) - Double.parseDouble(values[50]);
            double residue = timeZone % 60;
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
                if (Double.parseDouble(values[i]) <= 0) return false;
            }

            // # not empty
            // ORIGIN 14
            // ORIGIN_CITY_NAME 15
            // ORIGIN_STATE_ABR 16
            // ORIGIN_STATE_NM 18
            // DEST 23
            // DEST_CITY_NAME 24
            // DEST_STATE_ABR 25
            // DEST_STATE_NM 27
            int[] indSholdNotBeEmpty = {14, 15, 16, 18, 23, 24, 25, 27};
            for (int i : indSholdNotBeEmpty) {
                if (values[i].isEmpty()) return false;
            }

            // # for flights not canceled
            // CANCELLED 47
            boolean isCanceled = (Double.parseDouble(values[47]) == 1);
            // # ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
            // DEP_TIME 30
            // ARR_TIME 41
            // ACTUAL_ELAPSED_TIME 51
            if (!isCanceled) {
                double timeDiff = getMinDiff(values[41], values[30]) - Double.parseDouble(values[51]) - timeZone;
                if (timeDiff != 0) return false;

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
                boolean arrDel15 = (Double.parseDouble(values[44]) == 1);
                if (arrDelayNew >= 15 && !arrDel15) return false;
            }

            // finally, check the carrier field and price field
            // UNIQUE_CARRIER 6
            // AVG_TICKET_PRICE 109
            if (values[6].isEmpty()) return false;
            double avgTicketPrice = Double.parseDouble(values[109]);

        } catch (NumberFormatException ex) {
            // ex.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Calculate the difference between two time stamp in minutes.
     * Note that the first timestamp is always later than the second one.
     * @param t1 first timestamp in "HHmm" format
     * @param t2 second timestamp in "HHmm" format
     * @return difference between t1 and t2 in minutes
     */
    public int getMinDiff(String t1, String t2) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("HHmm");
            Date date1 = format.parse(t1);
            Date date2 = format.parse(t2);
            long timeDiff = (date1.getTime() - date2.getTime()) / 60000;
            if (timeDiff <= 0) {
                timeDiff += 24 * 60;
            }

            return (int) timeDiff;
        } catch (ParseException ex) {
            return -1;
        }
    }

    /**
     * Get a list of carrier average price pairs in ascending order from a price map
     * @param priceMap key is the airline carrier ID, value is a list of two floats where
     *                 the first one is the number of flights, the second one is the sum
     *                 of all the flight ticket price.
     * @return a list of string array which contains the carrier ID and the average ticket
     *         price, the list is in ascending order.
     */
    public List<String[]> getSortedPrices(HashMap<String, List<Double>> priceMap) {
        List<String[]> priceList = new ArrayList<>();
        for (String key : priceMap.keySet()) {
            double avgPrice = priceMap.get(key).get(1) / priceMap.get(key).get(0);
            priceList.add(new String[]{key, String.format("%.2f", avgPrice)});
        }

        Comparator<String[]> comp = new Comparator<String[]>() {
            @Override
            public int compare(String[] o1, String[] o2) {
                if (Double.parseDouble(o1[1]) > (Double.parseDouble(o2[1]))) {
                    return 1;
                } else {
                    return -1;
                }
            }
        };
        Collections.sort(priceList, comp);
        return priceList;
    }

    /**
     * Print the price list
     * @param priceList a list of string array which contains the carrier ID and the average ticket
     *                  price, the list is in ascending order.
     */
    public void printPriceList(List<String[]> priceList) {
        for (String[] strs : priceList) {
            System.out.format("%s %s%n", strs[0], strs[1]);
        }
    }
}