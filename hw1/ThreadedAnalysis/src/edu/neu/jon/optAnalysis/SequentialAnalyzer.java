package edu.neu.jon.optAnalysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class SequentialAnalyzer {

    private Map<String, List<Double>> priceMap;
    private long k; // lines of bad records
    private long f; // lines of good records

    public long getK() {
        return this.k;
    }

    public long getF() {
        return this.f;
    }

    public SequentialAnalyzer() {
        initialize();
    }

    public void analyze(String path) {
        analyze(path, true);
    }

    public void analyze(String path, boolean rst) {
        if (rst) {
            initialize();
        }

        List<String[]> records = DataPreprocessor.unzipAndParseCSV(path);

        String carrier;
        Double price;
        List<Double> priceList;

        for (String[] r : records) {
            if (!sanityCheck(r)) {
                this.k++;
            } else {
                this.f++;
                carrier = r[OTPConsts.UNIQUE_CARRIER];
                price = Double.parseDouble(r[OTPConsts.AVG_TICKET_PRICE]);
                if (!this.priceMap.containsKey(carrier)) {
                    priceList = new ArrayList<Double>();
                    priceList.add(price);
                    priceMap.put(carrier, priceList);
                } else {
                    priceMap.get(carrier).add(price);
                }
            }
        }
    }

    public void printResults() {
        Map<String, Double> meanMap = getMean();
//        Map<String, Double> medianMap = getMedian();
        double mean;
        double median = 0;

        System.out.println(this.k);
        System.out.println(this.f);
        for (String k : this.priceMap.keySet()) {
            mean = meanMap.get(k);
//            median = medianMap.get(k);
            System.out.format("%s %.2f %.2f%n", k, mean, median);
        }
    }

    private void initialize() {
        this.k = 0;
        this.f = 0;
        this.priceMap = new HashMap<String, List<Double>>();
    }

    public Map<String, Double> getMedian() {
        sortPrices();
        Map<String, Double> medianMap = new HashMap<String, Double>();
        for (String k : this.priceMap.keySet()) {
            int medianIndex = this.priceMap.get(k).size() / 2;
            medianMap.put(k, this.priceMap.get(k).get(medianIndex));
        }
        return medianMap;
    }

    private void sortPrices() {
        for (String k : this.priceMap.keySet()) {
            Collections.sort(this.priceMap.get(k));
        }
    }

    public Map<String, Double> getMean() {
        Map<String, Double> meanMap = getSum();
        for (String k : this.priceMap.keySet()) {
            meanMap.put(k, meanMap.get(k) / this.priceMap.get(k).size());
        }
        return meanMap;
    }

    public Map<String, Double> getSum() {
        Map<String, Double> sumMap = new HashMap<String, Double>();
        for (String k : this.priceMap.keySet()) {
            sumMap.put(k, sumOfDoubleList(this.priceMap.get(k)));
        }
        return sumMap;
    }

    /**
     * Check the number of fields in the record and the logic between some fields
     * @param values a String array contains the value of each field in a record
     * @return true if the input is a valid record, false otherwise
     */
    public boolean sanityCheck(String[] values) {
        if (values.length != 110) return false;
        try {
            // check not 0
            for (int i : OTPConsts.NOTZERO) {
                if (Double.parseDouble(values[i]) == 0) return false;
            }

            double timeZone = getMinDiff(values[OTPConsts.CRS_ARR_TIME], values[OTPConsts.CRS_DEP_TIME])
                    - Double.parseDouble(values[OTPConsts.CRS_ELAPSED_TIME]);
            double residue = timeZone % 60;
            if (residue != 0) return false;

            // check larger than 0
            for (int i : OTPConsts.LARGERTHANZERO) {
                if (Double.parseDouble(values[i]) <= 0) return false;
            }

            // check not empty
            for (int i : OTPConsts.NOTEMPTY) {
                if (values[i].isEmpty()) return false;
            }

            // for flights not canceled
            boolean isCanceled = (Double.parseDouble(values[OTPConsts.CANCELLED]) == 1);

            // ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
            if (!isCanceled) {
                double timeDiff = getMinDiff(values[OTPConsts.ARR_TIME], values[OTPConsts.DEP_TIME])
                        - Double.parseDouble(values[OTPConsts.ACTUAL_ELAPSED_TIME]) - timeZone;
                if (timeDiff != 0) return false;

                double arrDelay = Double.parseDouble(values[OTPConsts.ARR_DELAY]);
                double arrDelayNew = Double.parseDouble(values[OTPConsts.ARR_DELAY_NEW]);
                // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
                if (arrDelay > 0) {
                    if (arrDelay != arrDelayNew) return false;
                }

                // if ArrDelay < 0 then ArrDelayMinutes???? should be zero
                if (arrDelay < 0) {
                    if (arrDelayNew != 0) return false;
                }
                // if ArrDelayMinutes >= 15 then ArrDel15 should be false
                boolean arrDel15 = (Double.parseDouble(values[OTPConsts.ARR_DEL15]) == 1);
                if (arrDelayNew >= 15 && !arrDel15) return false;
            }

            // finally, check the carrier field and price field
            if (values[OTPConsts.UNIQUE_CARRIER].isEmpty()) return false;
            double avgTicketPrice = Double.parseDouble(values[OTPConsts.AVG_TICKET_PRICE]);

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
        List<String[]> priceList = new ArrayList<String[]>();
        double avgPrice;
        double priceSum;
        for (String key : priceMap.keySet()) {
            priceSum = sumOfDoubleList(priceMap.get(key));
            avgPrice = priceSum / priceMap.get(key).get(0);
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
     *
     * @param lod
     * @return
     */
    public double sumOfDoubleList(List<Double> lod) {
        double sum = 0;
        for (double d : lod){
            sum += d;
        }
        return sum;
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