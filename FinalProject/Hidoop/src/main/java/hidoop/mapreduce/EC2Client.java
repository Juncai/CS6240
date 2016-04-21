package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.util.Consts;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jon on 4/9/16.
 */
public class EC2Client implements Client {
    private Configuration conf;
    private List<Path> inputPathList;
    private Map<Integer, Node> nodeMap;
    private Map<Integer, List<Integer>> nodeReducerMap;
//    private Map<Integer, Consts.TaskStatus> mapStatus;
//    private Map<Integer, Consts.TaskStatus> reduceStatus;
    private Consts.Stages status;
    private ListeningThread lt;
    private FileSystem fs;
    private Pool<Node> nodePool;
    private ReducerInputPool reducerPool;
    private ReducerInputPool reducerInputPool;
    private Counter MapOutputCounter;

    public EC2Client(Configuration conf) throws IOException {
        this.conf = conf;
        fs = FileSystem.get(conf);
        initialize();
    }

    private void initialize() {
        status = Consts.Stages.DEFINE;
        nodeMap = new HashMap<Integer, Node>();
        nodeReducerMap = new HashMap<Integer, List<Integer>>();
//        mapStatus = new HashMap<Integer, Consts.TaskStatus>();
//        reduceStatus = new HashMap<Integer, Consts.TaskStatus>();
        int numMapperTasks = inputPathList.size();
        int numReducers = (numMapperTasks / 2) > 1 ? numMapperTasks / 2 : 1;
        conf.setNumReduceTasks(numReducers);
        int numSlaves = conf.slaveIpList.size();
        this.MapOutputCounter = new Counter();

        // initialize node
        for (int i = 0; i < numSlaves; i++) {
            nodeMap.put(i, new Node(i, conf.slaveIpList.get(i)));
        }
        // create node nodePool;
        nodePool = new Pool<Node>(numSlaves);

        // initialize Mapper
        // get the input file list
        inputPathList = fs.getFileList(new Path(conf.inputPath));
//        for (int i = 0; i < numMapperTasks; i++) {
//            mapStatus.put(i, Consts.TaskStatus.DEFINE);
//        }

        // initialize Reducer
        for (int i = 0; i < conf.reducerNumber; i++) {
//            reduceStatus.put(i, Consts.TaskStatus.DEFINE);
            int nodeInd = i % numSlaves;
            if (nodeReducerMap.containsKey(nodeInd)) {
                nodeReducerMap.put(nodeInd, new ArrayList<Integer>());
            }
            nodeReducerMap.get(nodeInd).add(i);
        }
        // create reducer input pool
        reducerInputPool = new ReducerInputPool(conf.reducerNumber, numMapperTasks);
        // create reducer pool
        reducerPool = new ReducerInputPool(conf.reducerNumber, 1);
    }

    @Override
    public void submitJob() throws IOException, InterruptedException {
        // start listening thread
        lt = new ListeningThread(conf.masterPort);
        lt.start();
        Node n;
        for (Path input : inputPathList) {
            n = nodePool.getResource();
            startMapOnSlave(n, input, conf.reducerNumber);
        }
        // TODO run whatever reducer is ready

        // all maps are done
        nodePool.waitTillAllAvailable();
        // all reducer inputs are ready
        reducerInputPool.waitTillAllAvailable();
        for (int i = 0; i < conf.slaveNum; i++) {
            startReduceOnSlave(nodeMap.get(i), nodeReducerMap.get(i));
        }
        // wait for all the reducers finished
        reducerPool.waitTillAllAvailable();
        // TODO finish the job
    }

    private void startMapOnSlave(Node n, Path input, int reducerNum) throws IOException {
        String header = Consts.RUN_MAP + " " + input.toString() + " " + reducerNum;
        sendInstruction(n, null, header);
    }

    private void startReduceOnSlave(Node n, List<Integer> reduceInds) throws IOException {
        String header = Consts.RUN_REDUCE;
        for (int redInd : reduceInds) {
            header += " " + redInd;
        }
        sendInstruction(n, null, header);
    }
    @Override
    public Counter getCounter(){
       return this.MapOutputCounter;
    }

    private void sendInstruction(Node n, List<String> data, String header) throws IOException {
        // TODO add node index in the header
        SendDataThread sdt = new SendDataThread(n.ip, conf.slavePort, header, data);
        sdt.start();
    }

    @Override
    public Consts.Stages getStatus() throws IOException, InterruptedException {
        return status;
    }

    class SendDataThread extends Thread {
        private String ip;
        private int port;
        private List<String> payload;
        private String header;

        public SendDataThread(String ip, int port, String header, List<String> payload) {
            this.ip = ip;
            this.port = port;
            this.payload = payload;
            this.header = header;
        }

        public void run() {
            try {
                Socket s = new Socket(ip, port);
                BufferedWriter wtr = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(),
                        "UTF-8"));
                // header
                header += Consts.END_OF_LINE;
                System.out.println("sending header: " + header);
                wtr.write(header);
                wtr.flush();
                wtr.close();
                s.close();
                // clean the payload list
//                payload = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
                    handleSlaveRunning(header);
                } else if (header[0].equals(Consts.MAP_DONE)) {
                    handleMapDone(header);
                } else if (header[0].equals(Consts.REDUCER_INPUT_READY)) {
                    handleReducerInputReady(header);
                } else if (header[0].equals(Consts.REDUCE_DONE)) {
                    handleReduceDone(header);
                }
                rdr.close();
                // close the connection on client side
//                s.close();
            } catch (Exception ee) {
                System.err.println("Error in ReceiveDataThread: " + ee.toString());
            }
        }

        private void handleSlaveRunning(String[] header) {
            // format: RUNNING NODE_INDEX
            nodePool.putResource(nodeMap.get(Integer.parseInt(header[1])));
        }

        private void handleMapDone(String[] header) {
            // format: MAP_DONE NODE_INDEX
            nodePool.putResource(nodeMap.get(Integer.parseInt(header[1])));
        }

        private void handleReducerInputReady(String[] header) {
            // format: REDUCER_INPUT_DONE REDUCER_INDEX MAP_INDEX
            int rInd = Integer.parseInt(header[1]);
            int mInd = Integer.parseInt(header[2]);
            reducerInputPool.putInput(rInd, mInd);
        }

        private void handleReduceDone(String[] header) {
            // format: REDUCE_DONE REDUCE_INDEX
            int rInd = Integer.parseInt(header[1]);
            reducerPool.putInput(rInd, 0);
        }
    }


}
