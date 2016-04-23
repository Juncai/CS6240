package slave;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import hidoop.conf.Configuration;
import hidoop.fs.*;
import hidoop.util.Consts;

// Author: Jun Cai
public class SlaveCommunication {
    private ListeningThread lt;
    private int localPort;
    private String masterIp;
    private int masterPort;
    private int nodeInd;
    private boolean nodeEndStates;
    private Configuration conf;
    private FileSystem fs;

    public SlaveCommunication(int nodeInd, int port, String masterIp, int masterPort) {
        this.nodeInd = nodeInd;
        this.localPort = port;
        this.masterIp = masterIp;
        this.masterPort = masterPort;
        nodeEndStates = false;
        // TODO prepare file system
        fs = FileSystem.get(true);
    }

    public void start() throws IOException, InterruptedException {
        // start listening thread
        lt = new ListeningThread(localPort);
        lt.start();
        Thread.sleep(1000);

        // send ready info to master
        String header = Consts.RUNNING + " " + nodeInd;
        sendMsgToMaster(header);

    }

    private void sendMsgToMaster(String msg) throws IOException {
        sendDataToNode(masterIp, masterPort, msg, null);
    }
    // send map output to reduce input
//    public void sendDataToNode(int ind, List<String> data, String header) throws IOException {
//        // TODO add node index in the header
//        SendDataThread sdt = new SendDataThread(ind, header, data);
//        sdt.start();
//    }

    private void sendReducerInput(List<String> data, int reducerInd, int mapperInd) throws IOException {
        // format: REDUCER_INPUT REDUCER_INDEX MAPPER_INDEX
        int tarNodeInd = reducerInd % conf.slaveNum;
        String nodeIp = conf.slaveIpList.get(tarNodeInd);
        String header = Consts.REDUCER_INPUT + " " + reducerInd + " " + mapperInd;
        sendDataToNode(nodeIp, localPort, header, data);
    }

    private void sendDataToNode(String ip, int port, String header, List<String> data) throws IOException {
        SendDataThread sdt = new SendDataThread(ip, port, header, data);
        sdt.start();
    }

    public void endCommunication() throws InterruptedException {
        // waiting for listening thread
        nodeEndStates = true;
        lt.join();
    }

    class SendDataThread extends Thread {
        private List<String> payload;
        private String header;
        private String ip;
        private int port;

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
                // data
                if (payload != null) {
                    for (String d : payload) {
                        d += Consts.END_OF_LINE;
                        wtr.write(d);
                    }
                    // end of data
                    wtr.write(Consts.END_OF_DATA_EOL);
                }
                wtr.flush();

                // TODO if it's running message, read Configuration from master
                if (header.startsWith(Consts.RUNNING)) {
                    ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
                    conf = (Configuration) ois.readObject();
                    System.out.println("Configuration received with Mapper class: " + conf.mapperClass.getCanonicalName());
                    ois.close();
                    sendMsgToMaster(Consts.READY + " " + nodeInd);
                }

                wtr.close();
                s.close();
                // clean the payload list
                payload = null;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private class ListeningThread extends Thread {
        private ServerSocket listenSocket;

        ListeningThread(int port) throws IOException {
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


    private class WorkThread extends Thread {
        Socket s;

        WorkThread(Socket s) {
            this.s = s;
        }

        public void run() {
            try {
                BufferedReader rdr = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"));
                String line = rdr.readLine();
                System.out.println("see header: " + line);
                String[] header = line.split(" ");
                if (header[0].equals(Consts.RUN_MAP)) {
                    handleRunMap(header);
                } else if (header[0].equals(Consts.RUN_REDUCE)) {
                    handleRunReduce(header);
                } else if (header[0].equals(Consts.REDUCER_INPUT)) {
                    handleDataTransfer(header, rdr);
                }
                rdr.close();
                // close the connection on client side
//                s.close();
            } catch (Exception ee) {
                System.err.println("Error in ReceiveDataThread: " + ee.toString());
            }
        }

        private void handleRunMap(String[] header) throws IOException {
            // format: RUN_MAP MAPPER_INDEX INPUT_PATH
            int mInd = Integer.parseInt(header[1]);
            Path inputPath = new Path(header[2]);
            if (conf == null) {
                sendMsgToMaster(Consts.MAP_FAILED + " " + mInd + " conf not initialized");
                return;
            }

            MapperRunner mr = new MapperRunner(conf, fs, mInd, inputPath);
            if (!mr.run()) {
                sendMsgToMaster(Consts.MAP_FAILED + " " + mInd + " map run failed");
                return;
            }
            // TODO send data to reducer node
            // TODO use MAP to store the data can be memory inefficient, alternative is to write to local file!!!
            Map<Integer, List<String>> outputBuffer = mr.outputBuffer;
            for (int i = 0; i < conf.reducerNumber; i++) {
                sendReducerInput(outputBuffer.get(i), i, mInd);
            }
            // TODO send master the result of the Mapper
            // format: MAP_DONE NODE_INDEX MAP_INDEX MAP_COUNTER
            sendMsgToMaster(Consts.MAP_DONE + " " + nodeInd + " " + mInd + " " + mr.counter);
            // TODO collect all seen keys
            // TODO for reducer load balancing
        }

        private void handleDataTransfer(String[] header, BufferedReader br) throws IOException {
            // TODO buffer the data in local file system
            int reducerInd = Integer.parseInt(header[1]);
            int mapperInd = Integer.parseInt(header[2]);
            Path dir = new Path(Consts.REDUCE_INPUT_DIR_PRE + reducerInd);
            try {
                // TODO create a dir for that reducer if not existed
                FileSystem.createDirIfNotExisted(dir);
                Path inputPath = Path.appendDirFile(dir, Consts.REDUCE_INPUT_PREFIX + mapperInd);
                OutputStream os = fs.create(inputPath);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));

                String line;
                while (null != (line = br.readLine())) {
                    if (line.equals(Consts.END_OF_DATA)) {
                        break;
                    } else {
                        bw.write(line);
                        bw.write(Consts.END_OF_LINE_L);
                    }
                }
                bw.flush();
                bw.close();
                // TODO send master the result of the reducer input transfer
                sendMsgToMaster(Consts.REDUCER_INPUT_READY + " " + reducerInd + " " + mapperInd);
            } catch (NumberFormatException ex) {
                System.out.println("Bad data transfer.");
                sendMsgToMaster(Consts.REDUCER_INPUT_FAILED + " " + reducerInd + " " + mapperInd);
            }
        }

        private void handleRunReduce(String[] header) throws IOException {
            // format: RUN_REDUCE OUTPUT_PATH REDUCER_INDEX0 REDUCER_INDEX1 ...
            Path outputDir = new Path(header[1]);
            Path outputPath;
            int reducerInd;
            for (int i = 2; i < header.length; i++) {
                reducerInd = Integer.parseInt(header[i]);
                ReducerRunner rr = new ReducerRunner(conf, fs, reducerInd);
                if (!rr.run()) {
                    sendMsgToMaster(Consts.REDUCE_FAILED + " " + reducerInd + " reduce run failed");
                    continue;
                }
                // send output to s3
                // TODO use a new thread to do that
                outputPath = Path.appendDirFile(outputDir, rr.outputPath.toString());
                fs.uploadToS3(rr.outputPath, outputPath);

                // send master the result of the Reducer
                sendMsgToMaster(Consts.REDUCE_DONE + " " + reducerInd);
            }
        }
    }
}
