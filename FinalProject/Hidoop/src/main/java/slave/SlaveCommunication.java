package slave;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import hidoop.util.Consts;

// Author: Jun Cai
public class SlaveCommunication {
    //    private ServerSocket listenSocket;
    private ListeningThread lt;
    private List<String> ipList;
    private int listenPort;
    private String masterIP;
    private int masterPort;
    private int nodeInd;
    private boolean nodeEndStates;

    public SlaveCommunication(int listenPort, int nodeInd, List<String> ips) throws Exception {
        nodeEndStates = false;
        this.listenPort = listenPort;
        this.nodeInd = nodeInd;
        ipList = ips;
        lt = new ListeningThread(listenPort);
        lt.start();
        Thread.sleep(1000);
    }

    public void sendDataToNode(int ind, List<String> data, String header) throws IOException {
        // TODO add node index in the header
        header = nodeInd + " " + header;
        SendDataThread sdt = new SendDataThread(ind, header, data);
        sdt.start();
    }

    public void endCommunication() throws InterruptedException {
        // waiting for listening thread
        nodeEndStates = true;
        lt.join();
    }

    class SendDataThread extends Thread {
        private int n;
        private List<String> payload;
        private String header;

        public SendDataThread(int n, String header, List<String> payload) {
            this.n = n;
            this.payload = payload;
            this.header = header;
        }

        public void run() {
            try {
                Socket s = new Socket(ipList.get(n), listenPort);
                BufferedWriter wtr = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(),
                        "UTF-8"));
                // header
                if (payload != null) {
                    // for testing
                    header += " " + payload.size() + Consts.END_OF_LINE;
                } else {
                    header += Consts.END_OF_LINE;
                }
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
                wtr.close();
                s.close();
                // clean the payload list
                payload = null;
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
