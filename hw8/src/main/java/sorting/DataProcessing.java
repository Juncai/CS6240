package sorting;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

//Author: Vikas Boddu
public class DataProcessing {
    private List<Double> sampleTemps;
    public List<String> data;
    private List<Double> pivots;
    private int numOfNodes;
    private int nodeInd;
    public int badCount;
    public int dataCount;
    private static String[] sanityCheckSplits;

    public DataProcessing(int nNodes, int ind) {
        numOfNodes = nNodes;
        nodeInd = ind;
        data = new ArrayList<String>();
        sampleTemps = new ArrayList<Double>();
        pivots = new ArrayList<Double>();
        badCount = 0;
        dataCount = 0;
    }

    public void feedLine(String line) {
        if (line.startsWith(Consts.HEADER_START)) return;

        if (sanityCheck(line)) {
            data.add(line);
            dataCount++;
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
        sanityCheckSplits = line.split(Consts.COMMA);
        try {
            Integer.parseInt(sanityCheckSplits[Consts.WBAN_NUMBER]);
            Integer.parseInt(sanityCheckSplits[Consts.DATE]);
            Integer.parseInt(sanityCheckSplits[Consts.TIME]);
            Double.parseDouble(sanityCheckSplits[Consts.DRY_BULB_TEMP]);
        } catch (Exception ex) {
            return false;
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
        int i = 0;
        double cTemp;
        for (String v : data) {
            if (i++ % Consts.SAMPLE_BASE == 0) {
                cTemp = getTemp(v);
                sampleTemps.add(cTemp);
                res.add(cTemp + "");
            }
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
        for (String v : data) {
            cTemp = getTemp(v);
            done = false;
            for (int i = 0; i < pivots.size(); i++) {
                if (i == pivots.size() - 1) {
                    if (cTemp < pivots.get(i)) {
                        if (i != nodeInd) {
                            res.get(i).add(v);
                        } else {
                            dataRemain.add(v);
                        }
                        done = true;
                    }
                } else {
                    if (cTemp <= pivots.get(i)) {
                        if (i != nodeInd) {
                            res.get(i).add(v);
                        } else {
                            dataRemain.add(v);
                        }
                        done = true;
                    }
                }
            }
            if (!done) {
                res.get(numOfNodes - 1).add(v);
            }
        }

        // only keep the necessary data
        data.clear();
        data = dataRemain;

        return res;
    }

//    public static String arrayToString(String[] arr) {
//        // TODO concatenate the strings with COMMA
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < arr.length; i++) {
//            sb.append(arr[i]);
//            if (i < arr.length - 1) {
//                sb.append(Consts.DELIMITER);
//            }
//        }
//
//        return sb.toString();
//    }

    private double getTemp(String line) {
        String[] values = line.split(Consts.COMMA);
        return Double.parseDouble(values[Consts.DRY_BULB_TEMP]);
    }

    public void recvSamples(List<Double> samples) {
        double s;
        System.out.println("Sample received: " + samples.size());
        sampleTemps.addAll(samples);

//        for (String ss : samples) {
//            try {
//                s = Double.parseDouble(ss);
//                sampleTemps.add(s);
//            } catch (NumberFormatException ex) {
//                // something wrong
//            }
//        }
    }

    public void recvData(List<String> d) {
        System.out.println("Data received: " + d.size());
        data.addAll(d);
    }

    private void getPivots() {
        List<Double> sampleList = new ArrayList<Double>(sampleTemps);
        Collections.sort(sampleList);

        if (numOfNodes == 2) {
            pivots = findOnePivot(sampleList);
        } else {
            pivots = findSevenPivots(sampleList);
        }
        // for testing
        printPivots();
    }


    private void printPivots() {
        System.out.print("Pivots: ");
        for (double p : pivots) {
            System.out.print(p + " ");
        }
        System.out.println("");
    }

    private List<Double> findSevenPivots(List<Double> samples) {
//        List<Double> res = new ArrayList<Double>();
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

    private List<Double> findOnePivot(List<Double> samples) {
        List<Double> res = new ArrayList<Double>();
        res.add(median(samples));
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

//    public void unZipAll(String inputPath, String outputPath) {
//        inputPathTxt = outputPath;
//        File dir = new File(inputPath);
//        File[] directoryListing = dir.listFiles();
//        if (directoryListing != null) {
//            for (File aFile : directoryListing) {
//                if (aFile.toString().endsWith(".txt.gz")) {
//                    unZipOne(aFile, outputPath);
//                }
//            }
//        } else {
//            System.out.println("No .*.txt.gz present");
//        }
//    }
//
//    public void unZipOne(File gzip, String outputPath) {
//        try {
//            String outputTxtName = gzip.getName();
//            outputTxtName = outputTxtName.substring(0, outputTxtName.length() - 3);
//            outputTxtName = outputPath + outputTxtName;
//
//            FileInputStream fis = new FileInputStream(gzip);
//            GZIPInputStream gis = new GZIPInputStream(fis);
//            FileOutputStream fos = new FileOutputStream(outputTxtName);
//            byte[] buffer = new byte[1024];
//            int len;
//            while ((len = gis.read(buffer)) != -1) {
//                fos.write(buffer, 0, len);
//            }
//            fos.close();
//            gis.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void txtReadAll() {
//        File dir = new File(inputPathTxt);
//        File[] directoryListing = dir.listFiles();
//        if (directoryListing != null) {
//            for (File aFile : directoryListing) {
//                if (aFile.toString().endsWith(".txt")) {
//                    txtReadOne(aFile);
//                }
//            }
//        } else {
//            System.out.println("No .*.txt present");
//        }
//    }
//
//    public void txtReadOne(File inputFile) {
//        BufferedReader br = null;
//        String line = "";
//        String cvsSplitBy = ",";
//        try {
//            br = new BufferedReader(new FileReader(inputFile));
//            int i = 0;
//            while ((line = br.readLine()) != null) {
//                String[] parsedValues = line.split(cvsSplitBy);
//                if (i > 0) {
//                    Value desiredValues = new Value(parsedValues[0], parsedValues[1], parsedValues[2], parsedValues[8]);
//                    list.add(desiredValues);
//                    if (i % Consts.SAMPLE_FREQUENCY == 0) {
//                        sampleTemps.add(desiredValues.temp);
//                    }
//                }
//                i++;
//            }
//            if (br != null) {
//                br.close();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


}
