package sorting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
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
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

// Author:
public class Main {
    public static void main(String[] args) throws Exception {
        for (String s : args) {
            System.out.println(s);
        }
        // args: listening port, port serving master, peer ip list, current node index,
        // input bucket name, input keys file path, output bucket name
        int listenPort = Integer.parseInt(args[0]);
        int masterPort = Integer.parseInt(args[1]);
        String peerIpFilePath = args[2];
        int cInd = Integer.parseInt(args[3]);
        String inputBucket = args[4];
        String keysFilePath = args[5];
        String outputBucket = args[6];
        String outputKey = "part-" + cInd;


        // load peer ip list
        BufferedReader br = new BufferedReader(new FileReader(peerIpFilePath));
        List<String> ipList = new ArrayList<String>();
        String line = null;
        int nNodes = 0;
        while ((line = br.readLine()) != null) {
            if (nNodes++ != cInd) {
                ipList.add(line);
                System.out.println("adding ip to iplist: " + line);
            }
        }
        br.close();

        // create data processing object
        DataProcessing dp = new DataProcessing(nNodes, cInd);

        // create communication object and initialize it
        // This should be done ASAP to create the listening socket at about the same time
        System.out.println("Start creating NodeCommunication...");
        // TODO handle communication with master node
        NodeCommunication comm = new NodeCommunication(listenPort, masterPort, ipList);


        // load data from S3
//        AWSCredentials credentials = new EnvironmentVariableCredentialsProvider().getCredentials();
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        S3Object object;

        // TODO load input from s3, parse and sample the data
//        List<String> dataToSomeNode = new ArrayList<String>();
//        List<String> finalData = new ArrayList<String>();

        br = new BufferedReader(new FileReader(keysFilePath));
//        GZIPInputStream gis = new GZIPInputStream(fis);
        GZIPInputStream gis;
        BufferedReader inputBr;
        String dataLine;
        while ((line = br.readLine()) != null) {
            object = s3.getObject(new GetObjectRequest(inputBucket, line));
            gis = new GZIPInputStream(object.getObjectContent());
            inputBr = new BufferedReader(new InputStreamReader(gis));
            // add the first two line into data as a test
//            dataToSomeNode.add(inputBr.readLine());
//            dataToSomeNode.add(inputBr.readLine());
            System.out.println("reading data from s3");
            // feed data to the DataProcessing
            while ((dataLine = inputBr.readLine()) != null) {
                dp.feedLine(dataLine);
            }
        }
        br.close();

        // TODO sample local data
        System.out.println("Good data: " + dp.dataCount);
        System.out.println("Bad data: " + dp.badCount);
        Set<String> localSamples = dp.getLocalSamples();

        // send data to other nodes
        System.out.println("Start sending sample data...");
        Set<String> dataReceived;
        for (String ip : ipList) {
            comm.sendDataToNode(ip, localSamples);
        }
        // enter barrier, wait for other nodes getting ready for SELECT stage
        System.out.println("Entering barrier...");
        comm.barrier(Consts.Stage.SELECT);
        // load data from buffer
        System.out.println("Start reading sample data...");
        dataReceived = comm.readBufferedData();
//        finalData.addAll(dataReceived);
        dp.recvSamples(dataReceived);
        dataReceived.clear();


        // TODO choose pivots, prepare data for other nodes
        List<Set<String>> dataToOtherNodes = dp.dataToOtherNode();

        // send data to other nodes
        System.out.println("Start sending select data...");
        for (int i = 0; i < ipList.size(); i++) {
            // since we remove the current ip from the ip list
            int actualInd = (i >= cInd) ? i + 1 : i;
            comm.sendDataToNode(ipList.get(i), dataToOtherNodes.get(actualInd));
        }
        // enter barrier, wait for other nodes getting ready for SORT stage
        System.out.println("Entering barrier...");
        comm.barrier(Consts.Stage.SORT);
        // load data from buffer
        System.out.println("Start reading select data...");
        dataReceived = comm.readBufferedData();
        dp.recvData(dataReceived);
        dataReceived.clear();

        // TODO sort the local data, then send the result to S3
        List<String[]> outputData = dp.sortData();

        // create output file in the output bucket
        s3.deleteObject(outputBucket, outputKey);
        s3.putObject(new PutObjectRequest(outputBucket, outputKey,
                createOutputFile(outputKey, outputData)));

        // close sockets
        System.out.println("Closing connections...");
        comm.endCommunication();
    }

    private static File createOutputFile(String fileName, List<String[]> data) throws IOException {

        File file = new File(fileName);
        file.createNewFile();
//        file.deleteOnExit();
        FileWriter fw = new FileWriter(file, true);
        String line;
        for (String[] v : data) {
            line = DataProcessing.arrayToString(v);
            fw.write(line);
            fw.write("\n");
        }
        fw.flush();
        fw.close();

        return file;
    }
}
