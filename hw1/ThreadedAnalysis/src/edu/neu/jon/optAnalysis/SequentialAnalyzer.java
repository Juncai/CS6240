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

        DataPreprocessor dp = new DataPreprocessor();
        dp.unzipAndParseCSV(path);
        this.k = dp.getK();
        this.f = dp.getF();
        this.priceMap = dp.getResMap();
//        List<String[]> records = DataPreprocessor.unzipAndParseCSV(path);

//        String carrier;
//        Double price;
//        List<Double> priceList;
//
//        for (String[] r : records) {
//            if (!sanityCheck(r)) {
//                this.k++;
//            } else {
//                this.f++;
//                carrier = r[OTPConsts.UNIQUE_CARRIER];
//                price = Double.parseDouble(r[OTPConsts.AVG_TICKET_PRICE]);
//                if (!this.priceMap.containsKey(carrier)) {
//                    priceList = new ArrayList<Double>();
//                    priceList.add(price);
//                    priceMap.put(carrier, priceList);
//                } else {
//                    priceMap.get(carrier).add(price);
//                }
//            }
//        }
    }

    public void printResults() {
        Map<String, Double> meanMap = getMean();
        Map<String, Double> medianMap = getMedian();
        double mean;
        double median = 0;

        System.out.println(this.k);
        System.out.println(this.f);
        for (String k : this.priceMap.keySet()) {
            mean = meanMap.get(k);
            median = medianMap.get(k);
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