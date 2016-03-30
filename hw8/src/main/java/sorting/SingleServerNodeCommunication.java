package sorting;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 3/29/16.
 */
public class SingleServerNodeCommunication {
    //    private ServerSocket listenSocket;
    private ListeningThread lt;
    private List<String> ipList;
    private int listenPort;
    final private Object bufferLock;
    private SingleServerBarrier b;
    private Consts.Stage stage;
    private List<Double> sampleBuffer;
    private List<String> dataBuffer;
    private int nodeInd;
    private boolean[] sampleRecvStates;
    private boolean[] dataRecvStates;

    public SingleServerNodeCommunication(int listenPort, int nodeInd, List<String> ips) throws Exception {
        sampleBuffer = new ArrayList<Double>();
        dataBuffer = new ArrayList<String>();
        this.listenPort = listenPort;
        this.nodeInd = nodeInd;
        ipList = ips;
        b = new SingleServerBarrier(ips.size());
        bufferLock = new Object();
        sampleRecvStates = new boolean[ips.size()];
        dataRecvStates = new boolean[ips.size()];
        lt = new ListeningThread(listenPort);
        lt.start();
        stage = Consts.Stage.SAMPLE;
    }


    public void sendDataToNode(int ind, List<String> data) throws IOException {
        String header = "";
        if (stage == Consts.Stage.SAMPLE) {
            header += Consts.SAMPLE_HEADER;
        } else {
            header += Consts.DATA_HEADER;
        }
        header += " " + nodeInd;

        SendDataThread sdt = new SendDataThread(ind, header, data);
        sdt.start();
    }

    public void endCommunication() throws InterruptedException {
        // waiting for listening thread
        lt.join();
    }

    /***
     * Note: the initial stage of the program is SAMPLE, so the first Barrier call should be
     * with SELECT stage, which means all nodes are ready for SELECT stage
     *
     * @param s
     * @throws IOException
     */
    public void barrier(Consts.Stage s) throws IOException {
        // set current node to ready
        synchronized (bufferLock) {
            if (s == Consts.Stage.SELECT) {
                sampleRecvStates[nodeInd] = true;
            } else {
                dataRecvStates[nodeInd] = true;
            }
        }
        boolean done = readDataDoneForStage(s);
        while (!done) {
            done = readDataDoneForStage(s);
        }
        // send READY signal to all other nodes
        this.stage = s;
        b.nodeReady(nodeInd, stage);
        sendReadyToNodes(stage);

        // wait for other nodes
        b.waitForOtherNodes(stage);
    }

    private void sendReadyToNodes(Consts.Stage s) throws IOException {
        String stageHeader = (s == Consts.Stage.SELECT) ? Consts.SAMPLE_HEADER : Consts.DATA_HEADER;
        String header = Consts.READY_HEADER + " " + nodeInd + " " + stageHeader;
        SendDataThread sdt;
        for (int i = 0; i < ipList.size(); i++) {
            if (i == nodeInd) continue;
            sdt = new SendDataThread(i, header, null);
            sdt.start();
        }
    }

    public List<Double> readBufferedSamples() {
        if (stage == Consts.Stage.SELECT) {
            // TODO need to clear the buffer somehow?
            return sampleBuffer;
        }
        return null;
    }

    public List<String> readBufferedData() {
        if (stage == Consts.Stage.SORT) {
            // TODO need to clear the buffer somehow?
            return dataBuffer;
        }
        return null;
    }
    private boolean readDataDoneForStage(Consts.Stage s) {
        if (s == Consts.Stage.SELECT) {
            return allTrue(sampleRecvStates);
        } else if (s == Consts.Stage.SORT) {
            return allTrue(dataRecvStates);
        }
        return false;
    }

    private boolean allTrue(boolean[] ba) {
        boolean res = true;
        synchronized (bufferLock) {
            for (boolean b : ba) {
                res &= b;
            }
        }
        return res;
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
                wtr.write(header, 0, header.length());
                // data
                if (payload != null) {
                    for (String d : payload) {
                        d += Consts.END_OF_LINE;
                        wtr.write(d, 0, d.length());
                    }
                    // end of data
                    wtr.write(Consts.END_OF_DATA_EOL, 0, Consts.END_OF_DATA_EOL.length());
                }
                wtr.flush();
                wtr.close();
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
                if (header[0].equals(Consts.MASTER_HEADER)) {
                    handleMasterRequest(rdr);
                } else if (header[0].equals(Consts.SAMPLE_HEADER)) {
                    handleSampleTransfer(header, rdr);
                } else if (header[0].equals(Consts.DATA_HEADER)) {
                    handleDataTransfer(header, rdr);
                } else if (header[0].equals(Consts.READY_HEADER)) {
                    handleReadyMsg(header);
                }

            } catch (Exception ee) {
                System.err.println("Error in ReceiveDataThread: " + ee.toString());
            }
        }

        private void handleMasterRequest(BufferedReader br) {
            // TODO

        }

        private void handleReadyMsg(String[] header) {
            try {
                int nInd = Integer.parseInt(header[1]);
                if (header[2].equals(Consts.SAMPLE_HEADER)) {
                    b.nodeReady(nInd, Consts.Stage.SELECT);
                }
                if (header[2].equals(Consts.DATA_HEADER)) {
                    b.nodeReady(nInd, Consts.Stage.SORT);
                }
            } catch (Exception ex) {
                System.out.println("Bad ready signal.");
            }
        }

        private void handleDataTransfer(String[] header, BufferedReader br) throws IOException {
            if (stage != Consts.Stage.SELECT) return;
            try {
                int nInd = Integer.parseInt(header[1]);
                List<String> cBuffer = new ArrayList<String>();
                String line;
                while (null != (line = br.readLine())) {
                    if (line.equals(Consts.END_OF_DATA)) {
                        synchronized (bufferLock) {
                            dataBuffer.addAll(cBuffer);
                            dataRecvStates[nInd] = true;
                        }
                        break;
                    } else {
                        cBuffer.add(line);
                    }
                }
            } catch (NumberFormatException ex) {
                System.out.println("Bad data transfer.");
            }
        }

        private void handleSampleTransfer(String[] header, BufferedReader br) throws IOException {
            if (stage != Consts.Stage.SAMPLE) return;
            try {
                int nInd = Integer.parseInt(header[1]);
                List<Double> cBuffer = new ArrayList<Double>();
                String line;
                while (null != (line = br.readLine())) {
//                    System.out.println(line);
                    if (line.equals(Consts.END_OF_DATA)) {
                        synchronized (bufferLock) {
                            sampleBuffer.addAll(cBuffer);
                            sampleRecvStates[nInd] = true;
                        }
                        break;
                    } else {
                        if (DataProcessing.isDouble(line)) {
                            cBuffer.add(Double.parseDouble(line));
                        }
                    }
                }
                System.out.println("sample receiving done: " + nInd);
            } catch (NumberFormatException ex) {
                System.out.println("Bad sample transfer.");
            }
        }
    }
}
