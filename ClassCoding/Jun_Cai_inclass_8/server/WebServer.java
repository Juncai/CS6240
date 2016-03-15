package main;

import java.io.IOException;
import java.net.ConnectException;
import textsock.TextSocket;

// Author: Jun Cai
public class WebServer {
    public static void main(String[] args) throws IOException {
        int PORT_BASE = 10000;
        int n = Integer.parseInt(args[0]);
        int port = PORT_BASE + n;
        TextSocket.Server svr = new TextSocket.Server(port);

        TextSocket conn;
        while (null != (conn = svr.accept())) {
            String req = conn.getln();
            while (!conn.getln().equals("")) {
                // skip it.
            }

            int nReq = Integer.parseInt(req);
            if (nReq == 2 || nReq == n) {
                conn.putln("prime");
                conn.close();
                continue;
            }

            if (nReq == 1 || nReq % n == 0) {
                conn.putln("not prime");
                conn.close();
                continue;
            } else {
                // send the number to n+1 server and wait for the response
                // handle no server on the other side
                try {
                    TextSocket ts = new TextSocket("localhost", port + 1);
                    ts.putln(nReq + "");
                    ts.putln("");

                    for (String line : ts) {
                        System.out.println(line);
                        conn.putln(line);
                    }
                } catch (ConnectException e) {
                    conn.putln("unknown");
                    conn.close();
                    continue;
                }
                conn.close();
                continue;
            }
        }
    }
}
