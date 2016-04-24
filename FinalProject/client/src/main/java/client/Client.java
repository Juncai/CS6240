package client;

import java.io.*;
import java.net.Socket;

// Author: Jun Cai
public class Client {
    private int port;
    private String ip;
    private boolean finished;


    public Client(String ip, int port) throws IOException {
        this.port = port;
        this.ip = ip;
        finished = false;
    }

    public void waitForComplete() throws InterruptedException {
        QuerySlaveStatusThread qsst;
        while(!finished) {
            qsst = new QuerySlaveStatusThread();
            qsst.start();
            qsst.join();
            Thread.sleep(10000);
        }
    }

    class QuerySlaveStatusThread extends Thread {

        public QuerySlaveStatusThread() {
        }

        public void run() {
            try {
                Socket s = new Socket(ip, port);
                // send query
                BufferedWriter wtr = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(),
                        "UTF-8"));
                wtr.write(Consts.STATUS_HEADER_EOL);
                wtr.flush();

                // read the status
                BufferedReader rdr = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"));
                String line;
                line = rdr.readLine();
                if (line.equals(Consts.FINISHED)) {
                    finished = true;
                }
                wtr.close();
                rdr.close();
                s.close();
            } catch (IOException e) {
//                e.printStackTrace();
                finished = true;
            }
        }
    }

    public static void main(String[] argv) throws IOException, InterruptedException {
        Client client = new Client(argv[0], Integer.parseInt(argv[1]));
        client.waitForComplete();
        System.out.println("Job completed!");
    }
}
