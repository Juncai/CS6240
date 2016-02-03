package analysis;

import java.io.File;
import java.util.*;


public class SequentialAnalyzer {

    Map<String, CarrierData> carrierMap;
    long k; // lines of bad records
    long f; // lines of good records
    boolean fastMedian;
    List<CarrierData> topCarriers;
    boolean isMean;

    public long getK() {
        return this.k;
    }

    public long getF() {
        return this.f;
    }

    public Map<String, CarrierData> getCarrierMap() {
        return carrierMap;
    }

    public SequentialAnalyzer() {
        this.isMean = true;
        this.fastMedian = false;
        initialize();
    }

    public SequentialAnalyzer(boolean isMean, boolean fastMedian) {
        this.isMean = isMean;
        this.fastMedian = fastMedian;
        initialize();
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

        DataPreprocessor dp = new DataPreprocessor(carrierMap);
        dp.unzipAndParseCSV(path);
        this.k += dp.getK();
        this.f += dp.getF();
    }

    /**
     * Print K, F and list of [C p m]
     */
    public void printResults() {
        prepareResults();
//        System.out.println(this.k);
//        System.out.println(this.f);
        for (CarrierData c : topCarriers) {
            for (String m : c.monthPrices.keySet()) {
                if (isMean) {
                    System.out.format("%s %s %.2f%n", m, c.id, c.means.get(m));
                } else {
                    System.out.format("%s %s %.2f%n", m, c.id, c.medians.get(m));
                }
            }
        }
    }

    private void prepareResults() {
        removeInactiveCarriers();
        topCarriers = Utils.getTopCarriers(10, carrierMap);
        for (CarrierData c : topCarriers) {
            if (isMean) {
                Utils.getMeanForCarrier(c);
            } else {
                Utils.getMedianForCarrier(c, fastMedian);
            }
        }
    }

    private void removeInactiveCarriers() {
        List<String> cToRemove = new ArrayList<String>();
        for (String k : carrierMap.keySet()) {
            if (!carrierMap.get(k).isActive) {
                cToRemove.add(k);
            }
        }
        for (String k : cToRemove) {
            carrierMap.remove(k);
        }
    }

    /**
     * Clean all the previous processed data.
     */
    void initialize() {
        this.k = 0;
        this.f = 0;
        this.carrierMap = new HashMap<String, CarrierData>();
    }


}