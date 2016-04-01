package sorting;

import java.util.*;

//Author: Vikas Boddu, Jun Cai
public class DataProcessing {
    private List<Double> sampleTemps;
    public List<String> data;
    private List<Double> pivots;
    private int numOfNodes;
    private int nodeInd;
    public int badCount;
    public int dataCount;
    private static double cTmp;

    public DataProcessing(int nNodes, int ind) {
        numOfNodes = nNodes;
        nodeInd = ind;
        data = new ArrayList<String>();
        sampleTemps = new ArrayList<Double>();
        pivots = new ArrayList<Double>();
        badCount = 0;
        dataCount = 0;
        cTmp = 0.0;
    }

    public void feedLine(String line) {
        if (line.startsWith(Consts.HEADER_START)) return;

        // remove all the spaces in the data records
//        line = line.replaceAll("\\s", "");

        if (sanityCheck(line)) {
            data.add(line);
//            dataCount++;
            // do the sampling here
            if (dataCount++ % Consts.SAMPLE_BASE == 0) {
                sampleTemps.add(cTmp);
            }
        } else {
            badCount++;
        }
    }

    public static boolean isDouble(String line) {
        try {
            Double.parseDouble(line);
        } catch (Exception ex) {
            return false;
        }
        return true;
    }

    public static boolean sanityCheck(String line) {
        String[] sanityCheckSplits = line.split(Consts.COMMA);
        try {
            Integer.parseInt(sanityCheckSplits[Consts.WBAN_NUMBER]);
            Integer.parseInt(sanityCheckSplits[Consts.DATE]);
            Integer.parseInt(sanityCheckSplits[Consts.TIME]);
            cTmp = Double.parseDouble(sanityCheckSplits[Consts.DRY_BULB_TEMP]);
        } catch (Exception ex) {
            return false;
        } finally {
            sanityCheckSplits = null;
        }
        return true;
    }

    public List<String> sortData() {

        Collections.sort(data, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                // TODO compare dry bulb temp
                double temp1 = getTemp(o1);
                double temp2 = getTemp(o2);
                if (temp1 > temp2) {
                    return -1;
                } else if (temp1 < temp2) {
                    return 1;
                }
                return 0;
            }
        });

        return data;
    }

    public List<String> getLocalSamples() {
        List<String> res = new ArrayList<String>();
        for (Double v : sampleTemps) {
            res.add(v + "");
        }
        return res;
    }

    public List<List<String>> dataToOtherNode() {
        // compute the pivots
        getPivots();

        List<List<String>> res = new ArrayList<List<String>>();
        List<String> dataRemain = new ArrayList<String>();

        // create buckets for each node
        for (int i = 0; i < numOfNodes; i++) {
            res.add(new ArrayList<String>());
        }

        double cTemp;
        boolean done;
        Random r = new Random();
        for (String v : data) {
            cTemp = getTemp(v);
            done = false;
            for (int i = 0; i < pivots.size(); i++) {
                // try to improve load balancing
                if (cTemp < pivots.get(i) ||
                        (cTemp == pivots.get(i) && r.nextInt(10) < 5)) {
                    if (i != nodeInd) {
                        res.get(i).add(v);
                    } else {
                        dataRemain.add(v);
                    }
                    done = true;
                    break;
                }
            }
            if (!done) {
                if (nodeInd == numOfNodes - 1) {
                    dataRemain.add(v);
                } else {
                    res.get(numOfNodes - 1).add(v);
                }
            }
        }

        // only keep the necessary data
        data.clear();
        data = dataRemain;

        return res;
    }

    private double getTemp(String line) {
        return Double.parseDouble(line.split(Consts.COMMA)[Consts.DRY_BULB_TEMP]);
    }

    public void recvSamples(List<Double> samples) {
        System.out.println("Sample received: " + samples.size());
        sampleTemps.addAll(samples);
    }

    private void getPivots() {
        Collections.sort(sampleTemps);

        if (numOfNodes == 2) {
            pivots = findOnePivot();
        } else {
//            pivots = findSevenPivots();
            pivots = findSevenPivotsJun();
        }
        // for testing
        printPivots();
        // clear samples
        sampleTemps = null;
    }


    private void printPivots() {
        System.out.print("Pivots: ");
        for (double p : pivots) {
            System.out.print(p + " ");
        }
        System.out.println("");
    }

    private List<Double> findSevenPivotsJun() {
        List<Double> res = new ArrayList<Double>();
        int interval = sampleTemps.size() / 8;
        for (int i = 1; i < 8; i++) {
            res.add(sampleTemps.get(interval * i));
        }
        return res;
    }

    private List<Double> findSevenPivots(List<Double> samples) {
        List<Double> res = new ArrayList<Double>();
        List<Double> samplesSplit0 = new ArrayList<Double>();
        List<Double> samplesSplit1 = new ArrayList<Double>();
        List<Double> samplesSplit2 = new ArrayList<Double>();
        List<Double> samplesSplit3 = new ArrayList<Double>();
        res.add(median(samples));

        if (samples.size() % 2 == 0) {
            samplesSplit0 = samples.subList(0, (samples.size() / 2));
            samplesSplit2 = samples.subList((samples.size() / 2), samples.size());
        } else {
            samplesSplit0 = samples.subList(0, ((samples.size() + 1) / 2) - 1);
            samplesSplit2 = samples.subList((samples.size() + 1) / 2, samples.size());
        }

        res.add(median(samplesSplit0));
        res.add(median(samplesSplit2));

        if (samplesSplit0.size() % 2 == 0) {
            samplesSplit1 = samplesSplit0.subList((samplesSplit0.size() / 2), samplesSplit0.size());
            samplesSplit0 = samplesSplit0.subList(0, (samplesSplit0.size() / 2));

            samplesSplit3 = samplesSplit2.subList((samplesSplit2.size() / 2), samplesSplit2.size());
            samplesSplit2 = samplesSplit2.subList(0, (samplesSplit2.size() / 2));
        } else {
            samplesSplit1 = samplesSplit0.subList((samplesSplit0.size() + 1) / 2, samplesSplit0.size());
            samplesSplit0 = samplesSplit0.subList(0, ((samplesSplit0.size() + 1) / 2) - 1);

            samplesSplit3 = samplesSplit2.subList((samplesSplit2.size() + 1) / 2, samplesSplit2.size());
            samplesSplit2 = samplesSplit2.subList(0, ((samplesSplit2.size() + 1) / 2) - 1);
        }

        res.add(median(samplesSplit0));
        res.add(median(samplesSplit1));
        res.add(median(samplesSplit2));
        res.add(median(samplesSplit3));

        Collections.sort(res);
        return res;
    }

    private List<Double> findOnePivot() {
        List<Double> res = new ArrayList<Double>();
        res.add(median(sampleTemps));
        return res;
    }

    private double median(List<Double> aL) {
        double median;
        if (aL.size() % 2 == 0) {
            median = (aL.get(aL.size() / 2) + aL.get(aL.size() / 2 - 1)) / 2;
        } else {
            median = aL.get(aL.size() / 2);
        }
        return median;
    }
}
