package sorting;

import java.io.IOException;
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
    private Map<String, Socket> writeConns;
    private Socket masterConn;
    private String masterAddr;
    private Object writeConnsLock;


    public NodeCommunication(String ip, int port, String masterAddr, List<String> peerAddr) throws IOException {
        listenSocket = new ServerSocket(port);
        this.masterAddr = masterAddr;
        masterConn = null;
        // initialize the connection maps
        readConns = new HashMap<String, Socket>();
        writeConns = new HashMap<String, Socket>();
        for (String pa : peerAddr) {
            readConns.put(pa, null);
            writeConns.put(pa, null);
        }
        writeConnsLock = new Object();
    }

    public void initCommunication() throws Exception {
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
        }
        catch (Exception ee) {
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
        }
        catch (Exception ee) {
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

