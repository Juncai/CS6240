package client;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

// Author: Jun Cai
public class Client {
    private List<String> ipList;
    private int port;
    private boolean[] nodesStatus;
    private final Object lock;

    public Client(String ipPath, int port) throws IOException {
        lock = new Object();
        this.port = port;
        loadIpList(ipPath);
        nodesStatus = new boolean[ipList.size()];
        initNodeStatus();
    }

    public boolean jobFinished() throws InterruptedException {
        initNodeStatus();
        QuerySlaveStatusThread qsst;
        List<QuerySlaveStatusThread> tList = new ArrayList<QuerySlaveStatusThread>();
        for (int i = 0; i < ipList.size(); i++) {
            qsst = new QuerySlaveStatusThread(i);
            tList.add(qsst);
            qsst.start();
        }

        for (QuerySlaveStatusThread t : tList) {
            t.join();
        }

        boolean finished = true;
        for (boolean b : nodesStatus) {
            finished &= b;
        }

        return finished;
    }

    private void initNodeStatus() {
        for (int i = 0; i < ipList.size(); i++) {
            nodesStatus[i] = false;
        }
    }

    private void loadIpList(String path) throws IOException {
        List<String> res = new ArrayList<String>();
        File f = new File(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String line;
        while (null != (line = br.readLine())) {
            res.add(line);
        }
        ipList = res;
    }


    class QuerySlaveStatusThread extends Thread {
        private int nodeInd;

        public QuerySlaveStatusThread(int nodeInd) {
            this.nodeInd = nodeInd;
        }

        public void run() {
            try {
                Socket s = new Socket(ipList.get(nodeInd), port);
                // send query
                BufferedWriter wtr = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(),
                        "UTF-8"));
                wtr.write(Consts.MASTER_HEADER_EOL);
                wtr.write(Consts.STATUS_REQ_EOL);
                wtr.flush();

                // read the status
                BufferedReader rdr = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"));
                String line;
                while (null != (line = rdr.readLine())) {
                    if (line.equals(Consts.FINISHED)) {
                        synchronized (lock) {
                            nodesStatus[nodeInd] = true;
                        }
                        break;
                    } else if (line.equals(Consts.WORKING)) {
                        break;
                    }
                }
                wtr.close();
                rdr.close();
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) {
        try {
            String ipPath = argv[0];
            int port = Integer.parseInt(argv[1]);
            Client client = new Client(ipPath, port);
            int status = client.jobFinished() ? 1 : 0;
            System.out.println(status + "");
        } catch (NumberFormatException ex) {
            System.out.println("Port Number should be an integer!");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
