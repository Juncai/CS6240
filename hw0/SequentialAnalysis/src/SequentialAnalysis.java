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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;
import org.apache.commons.csv.*;


public class SequentialAnalysis {
    private static final String GZIP_FILE = "C:\\Users\\jonca\\Downloads\\323.csv.gz";
    private static final List<String> FILEDS_LT_ZERO =
            Arrays.asList("ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID",
                    "ORIGIN_STATE_FIPS", "ORIGIN_WAC", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID",
                    "DEST_CITY_MARKET_ID", "DEST_STATE_FIPS", "DEST_WAC");
    private static final List<String> FILEDS_NOT_EMPTY =
            Arrays.asList("ORIGIN", "ORIGIN_CITY_NAME", "ORIGIN_STATE_ABR", "ORIGIN_STATE_NM");

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        SequentialAnalysis sa = new SequentialAnalysis();
        sa.unzipAndAnalyze(SequentialAnalysis.GZIP_FILE);

        long endTime = System.currentTimeMillis();
        System.out.println("Time used:");
        System.out.println((endTime - startTime) / 1000);
    }


    public void unzipAndAnalyze(String gzPath) {
        int[] stats;
        HashMap<String, List<Double>> priceMap = new HashMap<String, List<Double>>();

        try {
            FileInputStream fis = new FileInputStream(gzPath);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            InputStreamReader isr = new InputStreamReader(gzis);

            // parse csv file with different reader
//            stats = getPriceMapWithCSVParser(isr, priceMap);
            stats = getPriceMapWithBufferedReader(isr, priceMap);

        } catch (IOException ex) {
//            ex.printStackTrace();
            return;
        }

        // generate price list then sort it
        List<String[]> priceList = getSortedPrices(priceMap);


//        System.out.println(numOfFlights);
        System.out.println(stats[0]);
        System.out.println(stats[1]);
        printPriceList(priceList);
    }

    public int[] getPriceMapWithCSVParser(Reader reader, HashMap<String, List<Double>> priceMap) throws IOException {
        CSVParser cp = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
        int numOfFlights = 0;
        int k = 0;
        String carrier;
        double price;

        for (CSVRecord record : cp) {
            numOfFlights++;
            if (!sanityCheckRecord(record)) {
                System.out.println(numOfFlights - 1); // for debugging
                k++;
            } else {
                carrier = record.get("UNIQUE_CARRIER");
                price = getDoubleValue(record, "AVG_TICKET_PRICE");
                if (!priceMap.containsKey(carrier)) {
                    priceMap.put(carrier, Arrays.asList(1.0, price));
                } else {
                    List<Double> curList = priceMap.get(carrier);
                    curList.set(0, curList.get(0) + 1);
                    curList.set(1, curList.get(1) + price);
                }
            }
        }

        return new int[]{k, numOfFlights - k};
    }

    public boolean sanityCheckRecord(CSVRecord r) {
        try {
            // arr/dep time not 0, time zone is valid
            String crsArrTime = r.get("CRS_ARR_TIME");
            String crsDepTime = r.get("CRS_DEP_TIME");
            int crsElapsedTime = getIntValue(r, "CRS_ELAPSED_TIME");
            if (crsArrTime.equals("0") || crsDepTime.equals("0")) return false;
            int timeZone = getMinDiff(crsArrTime, crsDepTime) - crsElapsedTime;
            if (timeZone % 60 != 0) return false;

            // fields larger than 0
            for (String f : SequentialAnalysis.FILEDS_LT_ZERO) {
                if (getIntValue(r, f) <= 0) return false;
            }

            // fields not empty
            for (String f : SequentialAnalysis.FILEDS_NOT_EMPTY) {
                if (r.get(f).isEmpty()) return false;
            }

            //
            // # for flights not canceled
            // CANCELLED 47
            boolean canceled = (getIntValue(r, "CANCELLED") == 1);
            if (!canceled) {
                // # ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
                // DEP_TIME 30
                // ARR_TIME 41
                // ACTUAL_ELAPSED_TIME 51
                String depTime = r.get("DEP_TIME");
                String arrTime= r.get("ARR_TIME");
                int actualElapsedTime = getIntValue(r, "ACTUAL_ELAPSED_TIME");
                int timeDiff = getMinDiff(arrTime, depTime) - actualElapsedTime - timeZone;
                if (timeDiff != 0) return false;


                // ARR_DELAY 42
                // ARR_DELAY_NEW 43
                double arrDelay = getDoubleValue(r, "ARR_DELAY");
                double arrDelayNew = getDoubleValue(r, "ARR_DELAY_NEW");
                // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
                if (arrDelay > 0 && arrDelay != arrDelayNew) return false;

                //    if ArrDelay < 0 then ArrDelayMinutes???? should be zero
                if (arrDelay < 0 && arrDelayNew != 0) return false;

                //    if ArrDelayMinutes >= 15 then ArrDel15 should be false
                // ARR_DEL15 44
                boolean arrDel15 = (getDoubleValue(r, "ARR_DEL15") == 1);
                if (arrDelayNew >= 15 && !arrDel15) return false;
            }
        } catch (Exception ex) {
//            ex.printStackTrace();
            return false;
        }

        return true;
    }

    public int getMinDiff(String t1, String t2) {
        // time format: HHMM
        // return t1 - t2 in minutes, t1 should always later than t2
        try {
            SimpleDateFormat format = new SimpleDateFormat("HH:mm");
            Date date1 = format.parse(formatTime(t1));
            Date date2 = format.parse(formatTime(t2));
            long timeDiff = (date1.getTime() - date2.getTime()) / 6000;
            if (timeDiff <= 0) {
                timeDiff += 1440;
            }

            return (int) timeDiff;
        } catch (ParseException ex) {
            return -1;
        }
    }

    public List<String[]> getSortedPrices(HashMap<String, List<Double>> priceMap) {
        List<String[]> priceList = new ArrayList<>();
        for (String key : priceMap.keySet()) {
            Double avgPrice = priceMap.get(key).get(1) / priceMap.get(key).get(0);
            priceList.add(new String[]{key, avgPrice + ""});
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

    public void printPriceList(List<String[]> priceList) {
        for (String[] strs : priceList) {
            System.out.format("%s %.2f%n", strs[0], Double.parseDouble(strs[1]));
        }
    }

    public String formatTime(String t) {
        return t.substring(0, 2) + ":" + t.substring(2, 4);
    }

    public double getDoubleValue(CSVRecord r, String field) {
        return Double.parseDouble(r.get(field));
    }

    public int getIntValue(CSVRecord r, String field) {
        return Integer.parseInt(r.get(field));
    }

    public int[] getPriceMapWithBufferedReader(Reader reader, HashMap<String, List<Double>> priceMap) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        int numOfFlights = 0;
        int k = 0;
        String carrier;
        Double price;

        String header = br.readLine();
        String line;
        String[] values;
        while ((line = br.readLine()) != null) {
            numOfFlights++;
            // parse the line
//                System.out.println(line);
            values = parseCSVLine(line);
            if (!sanityCheck(values)) {
                System.out.println(numOfFlights - 1);
                k++;
            } else {
                // UNIQUE_CARRIER 6
                // AVG_TICKET_PRICE 110
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
            int timeZone = getMinDiff(values[40], values[29]) - Integer.parseInt(values[50]);
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

        } catch (NumberFormatException ex) {
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