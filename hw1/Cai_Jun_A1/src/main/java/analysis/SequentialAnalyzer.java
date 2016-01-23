package analysis;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class SequentialAnalyzer {

    Map<String, List<Double>> priceMap;
    long k; // lines of bad records
    long f; // lines of good records
    Set<String> eligibleCarrier;

    public long getK() {
        return this.k;
    }

    public long getF() {
        return this.f;
    }

    public Map<String, List<Double>> getPriceMap() {
        return this.priceMap;
    }

    public SequentialAnalyzer() {
        initialize();
    }

    public Set<String> getEligibleCarrier() {
        return this.eligibleCarrier;
    }

    /**
     * Analyze all the gzip files in the given directory.
     * @param dir directory which contain all the compressed csv files.
     */
    public void batchAnalyze(String dir) {
        for (File file : new File(dir).listFiles()) {
            analyze(file.getAbsolutePath(), false);
        }
    }

    /**
     * Analyze all the gzip files in the given list
     * @param files a compressed data file path list
     */
    public void batchAnalyze(List<String> files) {
        for (String file : files) {
            analyze(file, false);
        }
    }

    /**
     * Analyze the compressed csv data file given by the path. Clean all processed data.
     * @param path path of the commpressed csv data file
     * @throws InterruptedException
     */
    public void analyze(String path) throws InterruptedException {
        analyze(path, true);
    }

    /**
     * Analyze the compressed csv data file given by the path.
     * @param path path of the commpressed csv data file
     * @param rst if the processed data need to be cleaned
     */
    public void analyze(String path, boolean rst) {
        if (rst) {
            initialize();
        }

        DataPreprocessor dp = new DataPreprocessor(this.priceMap, this.eligibleCarrier);
        dp.unzipAndParseCSV(path);
        this.k += dp.getK();
        this.f += dp.getF();
    }

    /**
     * Print K, F and list of [C p m]
     */
    public void printResults() {
        Map<String, Double> meanMap = getMean();
        Map<String, Double> medianMap = getMedian();
        double mean;
        double median;

        System.out.println(this.k);
        System.out.println(this.f);
        for (String k : meanMap.keySet()) {
            mean = meanMap.get(k);
            median = medianMap.get(k);
            System.out.format("%s %.2f %.2f%n", k, mean, median);
        }
    }

    /**
     * Clean all the previous processed data.
     */
    void initialize() {
        this.k = 0;
        this.f = 0;
        this.priceMap = new HashMap<String, List<Double>>();
        this.eligibleCarrier = new HashSet<String>();
    }

    /**
     * Get median of ticket price for each eligible carrier.
     * @return a map contains carrier median pairs.
     */
    public Map<String, Double> getMedian() {
        sortPrices();
        Map<String, Double> medianMap = new HashMap<String, Double>();
        for (String k : this.eligibleCarrier) {
            medianMap.put(k, getMedian(this.priceMap.get(k)));
        }
        return medianMap;
    }

    /**
     * Get median from a sorted list of doubles
     * @param lod a sorted list of doubles
     * @return the median
     */
    public double getMedian(List<Double> lod) {
        int len = lod.size();
        if (len % 2 == 0) {
            return (lod.get(len / 2) + lod.get((len / 2) + 1)) / 2;
        } else {
            return lod.get((len / 2) + 1);
        }
    }

    /**
     * Sorting price list for eligible carrier.
     */
    private void sortPrices() {
        for (String k : this.eligibleCarrier) {
            Collections.sort(this.priceMap.get(k));
        }
    }

    /**
     * Get mean of ticket price for each eligible carrier.
     * @return a map contains carrier mean price pairs.
     */
    public Map<String, Double> getMean() {
        Map<String, Double> meanMap = getSum();
        for (String k : meanMap.keySet()) {
            meanMap.put(k, meanMap.get(k) / this.priceMap.get(k).size());
        }
        return meanMap;
    }

    /**
     * Get sum of all ticket price for each eligible carrier.
     * @return a map contains carrier sum of prices pairs.
     */
    public Map<String, Double> getSum() {
        Map<String, Double> sumMap = new HashMap<String, Double>();
        for (String k : this.eligibleCarrier) {
            sumMap.put(k, sumOfDoubleList(this.priceMap.get(k)));
        }
        return sumMap;
    }

    /**
     * Compute sum of all the elements in a double list.
     * @param lod a list of double instances
     * @return sum of all the doubles in the list
     */
    public double sumOfDoubleList(List<Double> lod) {
        double sum = 0;
        for (double d : lod){
            sum += d;
        }
        return sum;
    }
}