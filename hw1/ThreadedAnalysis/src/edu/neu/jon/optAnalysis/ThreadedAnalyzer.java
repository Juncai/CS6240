///**
// * Created by jonca on 1/12/2016.
// *
// * #### My result with input file 323.csv.gz ######
//     4082
//     435940
//     WN 57.79
//     HP 65.39
//     AS 74.96
//     PI 189.67
//     CO 203.56
//     EA 269.25
//     PA (1) 283.09
//     US 287.81
//     TW 295.68
//     AA 304.96
//     DL 346.31
//     UA 541.65
//     NW 53856.85
// * ################### Note about header handling ###################
// * I didn't include the header line as a record, so my K would be one
// * smaller than the reference result (4082 instead of 4083).
// *
// * ##################################################################
// *
// * Input argument: path to gzip file
// * Output:
// *      K (number of corrupted lines)
// *      F (number of sane flights)
// *      C p (where C is the carrier code, p is the mean price of tickets)
// */
//
//import java.io.*;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.zip.GZIPInputStream;
//
//
//public class SequentialAnalyer {
//
//    private Map<String, List<Double>> priceMap;
//    private long k; // lines of bad records
//    private long f; // lines of good records
//
//    public SequentialAnalyer() {
//        initialize();
//    }
//
//    public void analyze(List<String[]> records, boolean rst) {
//        if (rst) {
//            initialize();
//        }
//        foreach (String[] r : records) {
//
//        }
//    }
//
//    private void initialize() {
//        this.k = 0;
//        this.f = 0;
//        this.priceMap = new HashMap<String, List<Double>>();
//    }
//     /**
//     * Print the K, F and carrier price pairs in a sorted order
//     * @param k
//     * @param f
//     * @param priceMap
//     */
//    public void printSortedPriceList(int k, int f, Map<String, List<Double>> priceMap) {
//        // generate price list then sort it
//        List<String[]> priceList = getSortedPrices(priceMap);
//        System.out.println(stats[0]);
//        System.out.println(stats[1]);
//        printPriceList(priceList);
//    }
//
//
//   /**
//     * Get flight count and sum of ticket price for each carrier from a reader
//     * @param reader reader contains the unzipped stream
//     * @param priceMap key is the airline carrier ID, value is a list of two floats where
//     *                 the first one is the number of flights, the second one is the sum
//     *                 of all the flight ticket price.
//     * @return integer array contains number of corrupted records and sane flights
//     * @throws IOException
//     */
//    public int[] getPriceMapWithBufferedReader(Reader reader, HashMap<String, List<Double>> priceMap) throws IOException {
//        BufferedReader br = new BufferedReader(reader);
//        int numOfFlights = 0;
//        int k = 0;
//        String carrier;
//        Double price;
//
//        br.readLine();  // get rid of the header
//        String line;
//        String[] values;
//        while ((line = br.readLine()) != null) {
//            numOfFlights++;
//            values = parseCSVLine(line);
//            if (!sanityCheck(values)) {
//                k++;
//            } else {
//                carrier = values[UNIQUE_CARRIER];
//                price = Double.parseDouble(values[AVG_TICKET_PRICE]);
//                if (!priceMap.containsKey(carrier)) {
//                    List<Double> priceList = new ArrayList<Double>();
//                    priceList.add(price);
//                    priceMap.put(carrier, priceList);
//                } else {
//                    priceMap.get(carrier).add(price);
//                }
//            }
//        }
//
//        br.close();
//
//        return new int[]{k, numOfFlights - k};
//    }
//
//    /**
//     * Parse a line in CSV format
//     * @param line a string in CSV format
//     * @return list of values as a string array
//     */
//    public String[] parseCSVLine(String line) {
//        ArrayList<String> values = new ArrayList<>();
//        StringBuffer sb = new StringBuffer();
//        boolean inQuote = false;
//        char curChar;
//        for (int i = 0; i < line.length(); i++) {
//            curChar = line.charAt(i);
//            if (inQuote) {
//                if (curChar == '"') {
//                    inQuote = false;
//                } else {
//                    sb.append(curChar);
//                }
//
//            } else {
//                if (curChar == '"') {
//                    inQuote = true;
//                } else if (curChar == ',') {
//                    values.add(sb.toString());
//                    sb = new StringBuffer();
//                } else {
//                    sb.append(curChar);
//                }
//            }
//        }
//        values.add(sb.toString());  // last field
//
//        return values.toArray(new String[1]);
//    }
//
//    /**
//     * Check the number of fields in the record and the logic between some fields
//     * @param values a String array contains the value of each field in a record
//     * @return true if the input is a valid record, false otherwise
//     */
//    public boolean sanityCheck(String[] values) {
//        if (values.length != 110) return false;
//        try {
//            // check not 0
//            for (int i : indShouldNotBeZero) {
//                if (Double.parseDouble(values[i]) == 0) return false;
//            }
//
//            double timeZone = getMinDiff(values[CRS_ARR_TIME], values[CRS_DEP_TIME])
//                    - Double.parseDouble(values[CRS_ELAPSED_TIME]);
//            double residue = timeZone % 60;
//            if (residue != 0) return false;
//
//            // check larger than 0
//            for (int i : indShouldLargerThanZero) {
//                if (Double.parseDouble(values[i]) <= 0) return false;
//            }
//
//            // check not empty
//            for (int i : indSholdNotBeEmpty) {
//                if (values[i].isEmpty()) return false;
//            }
//
//            // for flights not canceled
//            boolean isCanceled = (Double.parseDouble(values[CANCELLED]) == 1);
//
//            // ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
//            if (!isCanceled) {
//                double timeDiff = getMinDiff(values[ARR_TIME], values[DEP_TIME])
//                        - Double.parseDouble(values[ACTUAL_ELAPSED_TIME]) - timeZone;
//                if (timeDiff != 0) return false;
//
//                double arrDelay = Double.parseDouble(values[ARR_DELAY]);
//                double arrDelayNew = Double.parseDouble(values[ARR_DELAY_NEW]);
//                // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
//                if (arrDelay > 0) {
//                    if (arrDelay != arrDelayNew) return false;
//                }
//
//                // if ArrDelay < 0 then ArrDelayMinutes???? should be zero
//                if (arrDelay < 0) {
//                    if (arrDelayNew != 0) return false;
//                }
//                // if ArrDelayMinutes >= 15 then ArrDel15 should be false
//                boolean arrDel15 = (Double.parseDouble(values[ARR_DEL15]) == 1);
//                if (arrDelayNew >= 15 && !arrDel15) return false;
//            }
//
//            // finally, check the carrier field and price field
//            if (values[UNIQUE_CARRIER].isEmpty()) return false;
//            double avgTicketPrice = Double.parseDouble(values[AVG_TICKET_PRICE]);
//
//        } catch (NumberFormatException ex) {
//            // ex.printStackTrace();
//            return false;
//        }
//        return true;
//    }
//
//    /**
//     * Calculate the difference between two time stamp in minutes.
//     * Note that the first timestamp is always later than the second one.
//     * @param t1 first timestamp in "HHmm" format
//     * @param t2 second timestamp in "HHmm" format
//     * @return difference between t1 and t2 in minutes
//     */
//    public int getMinDiff(String t1, String t2) {
//        try {
//            SimpleDateFormat format = new SimpleDateFormat("HHmm");
//            Date date1 = format.parse(t1);
//            Date date2 = format.parse(t2);
//            long timeDiff = (date1.getTime() - date2.getTime()) / 60000;
//            if (timeDiff <= 0) {
//                timeDiff += 24 * 60;
//            }
//
//            return (int) timeDiff;
//        } catch (ParseException ex) {
//            return -1;
//        }
//    }
//
//    /**
//     * Get a list of carrier average price pairs in ascending order from a price map
//     * @param priceMap key is the airline carrier ID, value is a list of two floats where
//     *                 the first one is the number of flights, the second one is the sum
//     *                 of all the flight ticket price.
//     * @return a list of string array which contains the carrier ID and the average ticket
//     *         price, the list is in ascending order.
//     */
//    public List<String[]> getSortedPrices(HashMap<String, List<Double>> priceMap) {
//        List<String[]> priceList = new ArrayList<String[]>();
//        double avgPrice;
//        double priceSum;
//        for (String key : priceMap.keySet()) {
//            priceSum = sumOfDoubleList(priceMap.get(key));
//            avgPrice = priceSum / priceMap.get(key).get(0);
//            priceList.add(new String[]{key, String.format("%.2f", avgPrice)});
//        }
//
//        Comparator<String[]> comp = new Comparator<String[]>() {
//            @Override
//            public int compare(String[] o1, String[] o2) {
//                if (Double.parseDouble(o1[1]) > (Double.parseDouble(o2[1]))) {
//                    return 1;
//                } else {
//                    return -1;
//                }
//            }
//        };
//        Collections.sort(priceList, comp);
//        return priceList;
//    }
//
//    /**
//     *
//     * @param lod
//     * @return
//     */
//    public double sumOfDoubleList(List<Double> lod) {
//        double sum = 0;
//        for (double d : lod){
//            sum += d;
//        }
//        return sum;
//    }
//    /**
//     * Print the price list
//     * @param priceList a list of string array which contains the carrier ID and the average ticket
//     *                  price, the list is in ascending order.
//     */
//    public void printPriceList(List<String[]> priceList) {
//        for (String[] strs : priceList) {
//            System.out.format("%s %s%n", strs[0], strs[1]);
//        }
//    }
//}