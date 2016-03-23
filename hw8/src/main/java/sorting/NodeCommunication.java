package sorting;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jon on 3/23/16.
 */
public class NodeCommunication {
    private ServerSocket listenSocket;
    private Map<String, Socket> readConns;
    private Map<String, ReceiveDataTread> listeningThreads;
    private Map<String, Socket> writeConns;
    private Socket masterConn;
    private String masterAddr;
    private Object writeConnsLock;
    private Barrier b;
    private Consts.Stage stage;


    // TODO handle communication with master node
    public NodeCommunication(int port, String masterAddr, List<String> peerAddr) throws Exception {
        stage = Consts.Stage.SAMPLE;
        listenSocket = new ServerSocket(port);
        this.masterAddr = masterAddr;
        this.b = new Barrier(peerAddr);
        masterConn = null;
        // initialize the connection maps
        readConns = new HashMap<String, Socket>();
        listeningThreads = new HashMap<String, ReceiveDataTread>();
        writeConns = new HashMap<String, Socket>();
        for (String pa : peerAddr) {
            readConns.put(pa, null);
            writeConns.put(pa, null);
        }
        writeConnsLock = new Object();
        initCommunication();
    }

    private void initCommunication() throws Exception {
        // init read conns
        InitReadConnThread irct = new InitReadConnThread(listenSocket, readConns);
        irct.start();
        Thread.sleep(5000);    // wait for other nodes to start

        // init write conns
        InitWriteConnThread cIwct;
        List<InitWriteConnThread> iwctList = new ArrayList<InitWriteConnThread>();
        for (String tarAddr : writeConns.keySet()) {
            cIwct = new InitWriteConnThread(writeConns, tarAddr, writeConnsLock);
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
        masterConn.close();
    }

    public void sendDataToNode(String tar, List<String> buffer) throws Exception {
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
            } else  if (stage == Consts.Stage.SORT) {
                allBufferedData.addAll(rdt.selectBuffer);
                rdt.selectBuffer.clear();
            }
        }
        return allBufferedData;
    }

    /***
     * Note: the initial stage of the program is SAMPLE, so the first Barrier call should be
     * with SELECT stage, which means all nodes are ready for SELECT stage
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
        // send READY signal to other nodes
        this.stage = stage;
        sendReadyToNodes();

        // wait for other nodes
        b.waitForOtherNodes();
    }

    private void sendReadyToNodes() throws IOException {
        BufferedWriter wtr;
        for (String adr: writeConns.keySet()) {
             wtr = new BufferedWriter(new OutputStreamWriter(writeConns.get(adr).getOutputStream(),
                    "UTF-8"));
            wtr.write(Consts.NODE_READY);
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
    public Object lock;
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
                        b.nodeReady(tarAddr);
                    } else {
                        if (currentBuffer != null) {
                            currentBuffer.add(line);
                        }
                    }
                }
            }
        } catch (Exception ee) {
            System.err.println("Error in ListenThread: " + ee.toString());
        }
    }

    private void changeStage() {
        synchronized (lock) {
            switch (stage) {
                case SAMPLE:
                    stage = Consts.Stage.SELECT;
                    currentBuffer = selectBuffer;
                    break;
                case SELECT:
                    stage = Consts.Stage.SORT;
                    currentBuffer = null;
            }
        }
    }
}

class SendDataTread extends Thread {
    private Socket writeConn;
    private List<String> payload;

    public SendDataTread(Socket writeConn, List<String> payload) throws Exception {
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
            wtr.write(Consts.END_OF_DATA, 0, Consts.END_OF_DATA.length());
            wtr.flush();
//            wtr.close();
        } catch (Exception ee) {
            System.err.println("Error in ListenThread: " + ee.toString());
        }
    }
}

class InitWriteConnThread extends Thread {
    private Map<String, Socket> writeConns;
    final private Object mapLock;
    private String tartAddr;

    public InitWriteConnThread(Map<String, Socket> writeConns, String tarAddr, Object mapLock) throws Exception {
        this.writeConns = writeConns;
        this.mapLock = mapLock;
        this.tartAddr = tarAddr;
        if (!writeConns.containsKey(tarAddr)) {
            throw new Exception("Target address is not in the writeConns");
        }
    }

    @Override
    public void run() {
        try {
            String[] parts = tartAddr.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);
            Socket conn = new Socket(ip, port);
            synchronized (mapLock) {
                writeConns.put(tartAddr, conn);
            }
        } catch (Exception ee) {
            System.err.println("Error in ListenThread: " + ee.toString());
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
            System.err.println("Error in ListenThread: " + ee.toString());
        }
    }

    void loop() throws Exception {
        Socket conn;
        String cAddr;
        while (null != (conn = listenSocket.accept())) {
            cAddr = conn.getInetAddress() + ":" + conn.getPort();
            if (readConns.containsKey(cAddr)) {
                readConns.put(cAddr, conn);
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

