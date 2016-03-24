package sorting;

import sorting.Value;
import sorting.Constants;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

//Author: Vikas Boddu
public class DataProcessing {
    String inputPathGzip;
    String inputPathTxt;
    ArrayList<Value> list = new ArrayList<Value>();
    ArrayList<Integer> sampleTemps = new ArrayList<Integer>();

    public DataProcessing(String inputPath) {
        this.inputPathGzip = inputPath;
    }

    public void unZipAll(String inputPath, String outputPath) {
        inputPathTxt = outputPath;
        File dir = new File(inputPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File aFile : directoryListing) {
                if (aFile.toString().endsWith(".txt.gz")) {
                    unZipOne(aFile, outputPath);
                }
            }
        } else {
            System.out.println("No .*.txt.gz present");
        }
    }

    public void unZipOne(File gzip, String outputPath) {
        try {
            String outputTxtName = gzip.getName();
            outputTxtName = outputTxtName.substring(0, outputTxtName.length() - 3);
            outputTxtName = outputPath + outputTxtName;

            FileInputStream fis = new FileInputStream(gzip);
            GZIPInputStream gis = new GZIPInputStream(fis);
            FileOutputStream fos = new FileOutputStream(outputTxtName);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
            fos.close();
            gis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void txtReadAll() {
        File dir = new File(inputPathTxt);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File aFile : directoryListing) {
                if (aFile.toString().endsWith(".txt")) {
                    txtReadOne(aFile);
                }
            }
        } else {
            System.out.println("No .*.txt present");
        }
    }

    public void txtReadOne(File inputFile) {
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try {
            br = new BufferedReader(new FileReader(inputFile));
            int i = 0;
            while ((line = br.readLine()) != null) {
                String[] parsedValues = line.split(cvsSplitBy);
                if (i > 0) {
                    Value desiredValues = new Value(parsedValues[0], parsedValues[1], parsedValues[2], parsedValues[8]);
                    list.add(desiredValues);
                    if (i % SAMPLE_FREQUENCY) {
                        sampleTemps.add(desiredValues.temp);
                    }
                }
                i++;
            }
            if (br != null) {
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendSamples() {
        //Networking
    }

    public void recvSamples() {
        //Networking
        sampleTemps.addAll(recvTemps);
    }

    public ArrayList<Integer> pivots(int numberOfNodes) {
        Collections.sort(sampleTemps);
        ArrayList<Double> pivots = new ArrayList<Double>();
        pivots.add(median(sampleTemps));
        //TODO split array and generate 1 or 7 medians depending on numberofnodes
    }

    public double median(ArrayList<Integer> aL) {
        double median;
        if (sampleTemps.size() % 2 == 0) {
            median = ((double) aL.get(aL.size() / 2) + (double) aL.get(aL.size() / 2 - 1)) / 2;
        } else {
            median = (double) aL.get(aL.size() / 2);
        }
        return median;
    }
}
