package slave;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
//import java.nio.file.Files;
//import java.nio.file.Paths;
import java.util.List;

import hidoop.conf.Configuration;
import hidoop.util.Consts;

// Author: Jun Cai
public class SlaveCommunication {
    //    private ServerSocket listenSocket;
    private ListeningThread lt;
    private int localPort;
    private String masterIp;
    private int masterPort;
    private int nodeInd;
    private boolean nodeEndStates;
    private Configuration conf;

    public SlaveCommunication(int nodeInd, int port, String masterIp, int masterPort) {
        this.nodeInd = nodeInd;
        this.localPort = port;
        this.masterIp = masterIp;
        this.masterPort = masterPort;
        nodeEndStates = false;
    }

    public void start() throws IOException, InterruptedException {
        // start listening thread
        lt = new ListeningThread(localPort);
        lt.start();
        Thread.sleep(1000);

        // send ready info to master
        sendRunningToMaster();


    }

    public void sendRunningToMaster() throws IOException {
        String header = Consts.RUNNING + " " + nodeInd;
        sendDataToNode(masterIp, masterPort, header, null);
    }
    // send map output to reduce input
//    public void sendDataToNode(int ind, List<String> data, String header) throws IOException {
//        // TODO add node index in the header
//        SendDataThread sdt = new SendDataThread(ind, header, data);
//        sdt.start();
//    }

    public void sendDataToNode(String ip, int port, String header, List<String> data) throws IOException {
        // TODO add node index in the header
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
                    conf = (Configuration)ois.readObject();
                    System.out.println("Configuration received with Mapper class: " + conf.mapperClass.getCanonicalName());
                    ois.close();
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
                boolean ret;
                if (header[0].equals(Consts.RUN_MAP)) {
                    MapperRunner mr = new MapperRunner(header);
                    ret = mr.run();
                    // TODO send master the result of the Mapper
                    // TODO collect all seen keys
                } else if (header[0].equals(Consts.RUN_PARTITION)) {
                    PartitionerRunner pr = new PartitionerRunner(header);
                    ret = pr.run();
                    // TODO send master the result of the Partitioner
                } else if (header[0].equals(Consts.RUN_REDUCE)) {
                    ReducerRunner rr = new ReducerRunner(header);
                    ret = rr.run();
                    // TODO send master the result of the Reducer
                } else if (header[0].equals(Consts.REDUCER_INPUT)) {
                    handleDataTransfer(header, rdr);
                    // TODO send master the result of the reducer input transfer
                }
                rdr.close();
                // close the connection on client side
//                s.close();
            } catch (Exception ee) {
                System.err.println("Error in ReceiveDataThread: " + ee.toString());
            }
        }

        private void handleDataTransfer(String[] header, BufferedReader br) throws IOException {
            // TODO buffer the data in local file system
            try {
                int nInd = Integer.parseInt(header[1]);
                // instead create a file
                File f = new File(Consts.BUFFER_FILE_PREFIX + nInd);
                Files.deleteIfExists(Paths.get(f.getPath()));
                f.createNewFile();
                FileWriter fw = new FileWriter(f, false);

                String line;
                while (null != (line = br.readLine())) {
                    if (line.equals(Consts.END_OF_DATA)) {
                        break;
                    } else {
                        fw.write(line);
                        fw.write(Consts.END_OF_LINE_L);
                    }
                }
                fw.flush();
                fw.close();
//                synchronized (bufferLock) {
//                    dataRecvStates[nInd] = true;
//                }
            } catch (NumberFormatException ex) {
                System.out.println("Bad data transfer.");
            }
        }
    }
}
