package sorting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

// Author: Jun Cai
public class Main {
    public static void main(String[] args) throws Exception {
        long startTSAll;
        long startTS;
        long endTSAll;
        long endTS;
        for (String s : args) {
            System.out.println(s);
        }
        startTSAll = new Date().getTime();
        // args: listening port, port serving master, peer ip list, current node index,
        // input bucket name, input keys file path, output bucket name
        int listenPort = Integer.parseInt(args[0]);
        String peerIpFilePath = args[1];
        int cInd = Integer.parseInt(args[2]);
        String inputBucketPath = args[3];
        String keysFilePath = args[4];
        String outputBucketPath = args[5];

        // parse the s3 urls
        String[] bucketInfo = Utils.extractBucketAndDir(inputBucketPath);
        String inputBucket = bucketInfo[0];
        String inputDir = bucketInfo[1];
        bucketInfo = Utils.extractBucketAndDir(outputBucketPath);
        String outputBucket = bucketInfo[0];
        String outputDir = bucketInfo[1];
        String outputKey = outputDir + "/" + "part-" + cInd;


        // load peer ip list
        BufferedReader br = new BufferedReader(new FileReader(peerIpFilePath));
        List<String> ipList = new ArrayList<String>();
        String line = null;
        int nNodes = 0;
        while ((line = br.readLine()) != null) {
            ipList.add(line);
            System.out.println("adding ip to iplist: " + line);
            nNodes++;
        }
        br.close();

        // create data processing object
        DataProcessing dp = new DataProcessing(nNodes, cInd);

        // create communication object and initialize it
        // This should be done ASAP to create the listening socket at about the same time
        System.out.println("Start creating NodeCommunication...");
        // TODO handle communication with master node
//        NodeCommunication comm = new NodeCommunication(listenPort, masterPort, ipList);
        SingleServerNodeCommunication comm = new SingleServerNodeCommunication(listenPort, cInd, ipList);


        // load data from S3
//        AWSCredentials credentials = new EnvironmentVariableCredentialsProvider().getCredentials();
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        S3Object object;

        // TODO load input from s3, parse and sample the data
        br = new BufferedReader(new FileReader(keysFilePath));
        GZIPInputStream gis;
        BufferedReader inputBr;
        String dataLine;
        while ((line = br.readLine()) != null) {
            if (line.trim().length() == 0) continue;
            // prepend dir
            object = s3.getObject(new GetObjectRequest(inputBucket, line));
            gis = new GZIPInputStream(object.getObjectContent());
            inputBr = new BufferedReader(new InputStreamReader(gis));
            System.out.println("reading data from s3");
            // feed data to the DataProcessing
            while ((dataLine = inputBr.readLine()) != null) {
                dp.feedLine(dataLine);
            }
            inputBr.close();
            gis.close();
            object.close();
        }
        br.close();

        // TODO sample local data
        System.out.println("Good data: " + dp.dataCount);
        System.out.println("Bad data: " + dp.badCount);
        startTS = new Date().getTime();
        List<String> localSamples = dp.getLocalSamples();

        // send data to other nodes
        System.out.println("Start sending sample data...");
        for (int i = 0; i < nNodes; i++) {
            if (i == cInd) continue;
            comm.sendDataToNode(i, localSamples);
        }
        // enter barrier, wait for other nodes getting ready for SELECT stage
        System.out.println("Entering barrier...");
        comm.barrier(Consts.Stage.SELECT);
        // clean localSamples
        localSamples = null;
        // load data from buffer
        System.out.println("Start reading sample data...");
        dp.recvSamples(comm.readBufferedSamples());
        // clean the sample buffer
        comm.sampleBuffer = null;


        // TODO choose pivots, prepare data for other nodes
        List<List<String>> dataToOtherNodes = dp.dataToOtherNode();

        // send data to other nodes
        System.out.println("Start sending select data...");
        for (int i = 0; i < nNodes; i++) {
            if (i == cInd) continue;
            comm.sendDataToNode(i, dataToOtherNodes.get(i));
        }
        // enter barrier, wait for other nodes getting ready for SORT stage
        System.out.println("Entering barrier...");
        comm.barrier(Consts.Stage.SORT);
        // clean data to other nodes
        dataToOtherNodes = null;

        // load data from buffer
        System.out.println("Start reading select data...");
        comm.readBufferedData(dp.data);

        // TODO sort the local data, then send the result to S3
        List<String> outputData = dp.sortData();
        endTS = new Date().getTime();
        System.out.println("Processing Start Time: " + startTS);
        System.out.println("Processing End Time: " + endTS);
        System.out.println("Time used: " + (endTS - startTS) / 1000);

        // create output file in the output bucket
        System.out.println("Start creating output...");
        if (nNodes == 2) {
            String outputKey0 = outputKey + "-0";
            String outputKey1 = outputKey + "-1";
            int nr0 = outputData.size() / 2;
            int nr1 = outputData.size() - nr0;
            s3.deleteObject(outputBucket, outputKey0);
            s3.deleteObject(outputBucket, outputKey1);
            s3.putObject(new PutObjectRequest(outputBucket, outputKey0,
                    createOutputFilePart(outputKey0, outputData, 0, nr0)));
            s3.putObject(new PutObjectRequest(outputBucket, outputKey1,
                    createOutputFilePart(outputKey1, outputData, nr0, nr1)));

        } else {
            s3.deleteObject(outputBucket, outputKey);
            s3.putObject(new PutObjectRequest(outputBucket, outputKey,
                    createOutputFile(outputKey, outputData)));

        }

        endTSAll = new Date().getTime();
        System.out.println("All Start Time: " + startTSAll);
        System.out.println("All End Time: " + endTSAll);
        System.out.println("Total Time used: " + (endTSAll - startTSAll) / 1000);

        // close sockets
        System.out.println("Closing connections...");
        // TODO find a good way to end the connections
        comm.endCommunication();
    }

    private static File createOutputFilePart(String fileName, List<String> data, int begin, int nRecords) throws IOException {
        System.out.println("Creating " + fileName);
        File file = new File(fileName);
        file.createNewFile();
        FileWriter fw = new FileWriter(file, true);
        String line;
        for (int i = 0; i < nRecords; i++) {
            line = data.get(begin + i);
            fw.write(line);
            fw.write("\n");
        }
        fw.flush();
        fw.close();

        return file;
    }

    private static File createOutputFile(String fileName, List<String> data) throws IOException {
        System.out.println("Creating " + fileName);
        File file = new File(fileName);
        file.createNewFile();
        FileWriter fw = new FileWriter(file, true);
        for (String v : data) {
            fw.write(v);
            fw.write("\n");
        }
        fw.flush();
        fw.close();

        return file;
    }
}
