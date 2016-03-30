package sorting;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

// Author: Jun Cai
public class NodeCommunication {
    private ServerSocket listenSocket;
    private List<String> peerIPs;
    private Map<String, Socket> readConns;
    private Map<String, ReceiveDataTread> listeningThreads;
    private Map<String, Socket> writeConns;
    private ServerSocket masterSocket;
    private int masterPort;
    private int listenPort;
    final private Object writeConnsLock;
    private Barrier b;
    private Consts.Stage stage;


    // TODO handle communication with master node
    public NodeCommunication(int listenPort, int masterPort, List<String> peerIPs) throws Exception {
        stage = Consts.Stage.SAMPLE;
        this.listenPort = listenPort;
        this.masterPort = masterPort;
        listenSocket = new ServerSocket(listenPort);
        masterSocket = new ServerSocket(masterPort);

        this.peerIPs = peerIPs;

        b = new Barrier(this.peerIPs);

        // initialize the connection maps
        readConns = new HashMap<String, Socket>();
        listeningThreads = new HashMap<String, ReceiveDataTread>();
        writeConns = new HashMap<String, Socket>();
        for (String ip : this.peerIPs) {
            readConns.put(ip, null);
            writeConns.put(ip, null);
        }
        writeConnsLock = new Object();
        initCommunication();
    }

    private void initCommunication() throws Exception {
        // init read conns
        System.out.println("initComm...");
        InitReadConnThread irct = new InitReadConnThread(listenSocket, readConns);
        irct.start();
        Thread.sleep(20000);    // wait for other nodes to start

        // init write conns
        InitWriteConnThread cIwct;
        List<InitWriteConnThread> iwctList = new ArrayList<InitWriteConnThread>();
        String remoteAddr;
        for (String tarIP : peerIPs) {
            remoteAddr = tarIP + ":" + this.listenPort;
            System.out.println("Init write conn with: " + remoteAddr);
            cIwct = new InitWriteConnThread(writeConns, remoteAddr, writeConnsLock);
            iwctList.add(cIwct);
            cIwct.start();
        }

        irct.join();
        for (InitWriteConnThread t : iwctList) {
            t.join();
        }

        // start listening to other nodes
        ReceiveDataTread rdt;
        for (String adr : readConns.keySet()) {
            rdt = new ReceiveDataTread(adr, readConns.get(adr), b);
            listeningThreads.put(adr, rdt);
            rdt.start();
        }
    }

    public void endCommunication() throws IOException {
        for (String ad : writeConns.keySet()) {
            writeConns.get(ad).close();
        }
        for (String ad : readConns.keySet()) {
            readConns.get(ad).close();
        }
        listenSocket.close();
        masterSocket.close();
    }

    public void sendDataToNode(String tar, Collection<String> buffer) throws Exception {
        // write data
        SendDataTread sdt = new SendDataTread(writeConns.get(tar), buffer);
        sdt.start();
    }

    public List<String> readBufferedData() {
        List<String> allBufferedData = new ArrayList<String>();
        ReceiveDataTread rdt;
        for (String adr : listeningThreads.keySet()) {
            rdt = listeningThreads.get(adr);
            if (stage == Consts.Stage.SELECT) {
                allBufferedData.addAll(rdt.sampleBuffer);
                rdt.sampleBuffer.clear();
            } else if (stage == Consts.Stage.SORT) {
                allBufferedData.addAll(rdt.selectBuffer);
                rdt.selectBuffer.clear();
            }
        }
        return allBufferedData;
    }

    /***
     * Note: the initial stage of the program is SAMPLE, so the first Barrier call should be
     * with SELECT stage, which means all nodes are ready for SELECT stage
     *
     * @param stage
     * @throws IOException
     */
    // TODO need to think about if this is a proper way to do the barrier; the other approach is
    // TODO to proceed when data from all other nodes are received.
    public void barrier(Consts.Stage stage) throws IOException {
        boolean done = readDataDoneForStage(stage);
        while (!done) {
            done = readDataDoneForStage(stage);
        }
        // send READY signal to all other nodes
        this.stage = stage;
        sendReadyToNodes();

        // wait for other nodes
        b.waitForOtherNodes();
    }

    private void sendReadyToNodes() throws IOException {
        BufferedWriter wtr;
        for (String adr : writeConns.keySet()) {
            wtr = new BufferedWriter(new OutputStreamWriter(writeConns.get(adr).getOutputStream(),
                    "UTF-8"));
            System.out.println("Sending Node ready: " + adr);
            String nready = Consts.NODE_READY + Consts.END_OF_LINE;
            wtr.write(nready);
            wtr.flush();
        }
    }

    private boolean readDataDoneForStage(Consts.Stage stage) {
        ReceiveDataTread rdt;
        for (String adr : listeningThreads.keySet()) {
            rdt = listeningThreads.get(adr);
            synchronized (rdt.lock) {
                if (rdt.stage != stage) {
                    return false;
                }
            }
        }
        return true;
    }
}

class ReceiveDataTread extends Thread {
    final public Object lock;
    private Socket readConn;
    public List<String> sampleBuffer;
    public List<String> selectBuffer;
    private List<String> currentBuffer;
    private Barrier b;
    private String tarAddr;
    public Consts.Stage stage;

    public ReceiveDataTread(String tarAddr, Socket readConn, Barrier b) throws Exception {
        stage = Consts.Stage.SAMPLE;
        lock = new Object();
        this.readConn = readConn;
        sampleBuffer = new ArrayList<String>();
        selectBuffer = new ArrayList<String>();
        currentBuffer = sampleBuffer;
        this.b = b;
        this.tarAddr = tarAddr;
    }

    @Override
    public void run() {
        try {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(readConn.getInputStream(), "UTF-8"));
            String line = rdr.readLine();
            while (true) {
                if (line != null) {
                    if (line.equals(Consts.END_OF_DATA)) {
                        changeStage();
                    } else if (line.equals(Consts.NODE_READY)) {
                        System.out.println("Node ready: " + tarAddr);
                        b.nodeReady(tarAddr);
                    } else {
                        if (currentBuffer != null) {
                            if (stage == Consts.Stage.SAMPLE
                                    && DataProcessing.isDouble(line)) {
                                currentBuffer.add(line);
                            }
                            if (stage == Consts.Stage.SELECT
                                    && DataProcessing.sanityCheck(line)) {
                                currentBuffer.add(line);
                            }
                        }
                    }
                }
                line = rdr.readLine();
            }
        } catch (Exception ee) {
            System.err.println("Error in ReceiveDataThread: " + ee.toString());
        }
    }

    private void changeStage() {
        synchronized (lock) {
            switch (stage) {
                case SAMPLE:
                    stage = Consts.Stage.SELECT;
//                    sampleBuffer.clear();
                    currentBuffer = selectBuffer;
                    break;
                case SELECT:
                    stage = Consts.Stage.SORT;
//                    selectBuffer.clear();
                    currentBuffer = null;
            }
        }
    }
}

class SendDataTread extends Thread {
    private Socket writeConn;
    private Collection<String> payload;

    public SendDataTread(Socket writeConn, Collection<String> payload) throws Exception {
        this.writeConn = writeConn;
        this.payload = payload;
    }

    @Override
    public void run() {
        try {
            BufferedWriter wtr = new BufferedWriter(new OutputStreamWriter(writeConn.getOutputStream(),
                    "UTF-8"));
            for (String s : payload) {
                s += Consts.END_OF_LINE;
                wtr.write(s, 0, s.length());
            }
            String eod = Consts.END_OF_DATA + Consts.END_OF_LINE;
            wtr.write(eod, 0, eod.length());
            wtr.flush();
//            wtr.close();
        } catch (Exception ee) {
            System.err.println("Error in SendDataThread: " + ee.toString());
        }
    }
}

class InitWriteConnThread extends Thread {
    private Map<String, Socket> writeConns;
    final private Object mapLock;
    private String tarIP;
    private int tarPort;

    public InitWriteConnThread(Map<String, Socket> writeConns, String tarAddr, Object mapLock) throws Exception {
        this.writeConns = writeConns;
        this.mapLock = mapLock;
        String[] parts = tarAddr.split(":");
        tarIP = parts[0];
        tarPort = Integer.parseInt(parts[1]);
        if (!writeConns.containsKey(tarIP)) {
            throw new Exception("Target address is not in the writeConns");
        }
    }

    @Override
    public void run() {
        try {
            Socket conn = new Socket(tarIP, tarPort);
            synchronized (mapLock) {
                writeConns.put(tarIP, conn);
            }
        } catch (Exception ee) {
            System.err.println("Error in InitWriteConnThread: " + ee.toString());
        }
    }
}


class InitReadConnThread extends Thread {
    final ServerSocket listenSocket;
    private Map<String, Socket> readConns;

    public InitReadConnThread(ServerSocket listenSocket, Map<String, Socket> readConns) {
        this.listenSocket = listenSocket;
        this.readConns = readConns;
    }

    @Override
    public void run() {
        try {
            loop();
        } catch (Exception ee) {
            System.err.println("Error in InitReadConnTread: " + ee.toString());
        }
    }

    void loop() throws Exception {
        Socket conn;
        String cAddr;
        while (null != (conn = listenSocket.accept())) {
            System.out.println("see connection from " + conn.getInetAddress().toString().substring(1));
            cAddr = conn.getInetAddress().toString().substring(1);
            System.out.println("Adding connection from " + cAddr);
            if (readConns.containsKey(cAddr)) {
                readConns.put(cAddr, conn);
                System.out.println("Added connection from " + cAddr);
            }
            if (done()) {
                return;
            }
        }
    }

    private boolean done() {
        for (String a : readConns.keySet()) {
            if (readConns.get(a) == null) {
                return false;
            }
        }
        return true;
    }
}

