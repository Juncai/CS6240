package hidoop.mapreduce;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import hidoop.conf.Configuration;
import hidoop.util.Consts;
import hidoop.util.InputUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jon on 4/9/16.
 */
public class EC2Client implements Client {
    private Configuration conf;
    private List<String> inputPathList;
    private Map<Integer, Consts.NodeStatus> nodeStatus;
    private Map<Integer, Consts.TaskStatus> mapStatus;
    private Map<Integer, Consts.TaskStatus> reduceStatus;
    private Consts.Stages status;
    private String[] inputBucketInfo;
    private String[] outputBucketInfo;
    private List<S3ObjectSummary> inputSummaryList;
    private ListeningThread lt;
    private Counter MapOutputCounter;

    public EC2Client(Configuration conf) throws IOException {
        this.conf = conf;
        status = Consts.Stages.DEFINE;
        nodeStatus = new HashMap<Integer, Consts.NodeStatus>();
        mapStatus = new HashMap<Integer, Consts.TaskStatus>();
        reduceStatus = new HashMap<Integer, Consts.TaskStatus>();
        this.MapOutputCounter = new Counter();

        // initialize node
        int ind = 0;
        for (String slaveIp : conf.slaveIpList) {
            nodeStatus.put(ind++, Consts.NodeStatus.OFFLINE);
        }

        // initialize Mapper
        // TODO get the input file list
        for (int i = 0; i < inputSummaryList.size(); i++) {
            mapStatus.put(i, Consts.TaskStatus.DEFINE);
        }

        // initialize Mapper
        for (int i = 0; i < conf.reducerNumber; i++) {
            reduceStatus.put(i, Consts.TaskStatus.DEFINE);
        }

        // start listening thread
        lt = new ListeningThread(conf.masterPort);
        lt.start();
    }

    @Override
    public Counter getCounter(){
       return this.MapOutputCounter;
    }
    @Override
    public void submitJob() throws IOException, InterruptedException {

    }

    @Override
    public Consts.Stages getStatus() throws IOException, InterruptedException {
        return status;
    }

    class ListeningThread extends Thread {
        private ServerSocket listenSocket;

        public ListeningThread(int port) throws IOException {
            listenSocket = new ServerSocket(port);
        }

        public void run() {
            Socket s;
            WorkThread wt;
            while (true) {
                try {
                    if (null != (s = listenSocket.accept())) {
                        wt = new WorkThread(s);
                        wt.start();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    class WorkThread extends Thread {
        Socket s;

        public WorkThread(Socket s) {
            this.s = s;
        }

        public void run() {
            try {
                BufferedReader rdr = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"));
                String line = rdr.readLine();
                System.out.println("see header: " + line);
                String[] header = line.split(" ");
                if (header[0].equals(Consts.RUNNING)) {

                }
                rdr.close();
                // close the connection on client side
//                s.close();
            } catch (Exception ee) {
                System.err.println("Error in ReceiveDataThread: " + ee.toString());
            }
        }

//        private void handleSampleTransfer(String[] header, BufferedReader br) throws IOException {
//            if (stage != Consts.Stage.SAMPLE) return;
//            try {
//                int nInd = Integer.parseInt(header[1]);
//                List<Double> cBuffer = new ArrayList<Double>();
//                String line;
//                while (null != (line = br.readLine())) {
////                    System.out.println(line);
//                    if (line.equals(Consts.END_OF_DATA)) {
//                        break;
//                    } else {
//                        if (DataProcessing.isDouble(line)) {
//                            cBuffer.add(Double.parseDouble(line));
//                        }
//                    }
//                }
//                synchronized (bufferLock) {
//                    sampleBuffer.addAll(cBuffer);
//                    sampleRecvStates[nInd] = true;
//                }
//                cBuffer = null;
//                System.out.println("sample receiving done: " + nInd);
//            } catch (NumberFormatException ex) {
//                System.out.println("Bad sample transfer.");
//            }
//        }
    }


}
