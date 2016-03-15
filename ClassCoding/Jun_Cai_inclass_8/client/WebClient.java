package main;

import java.io.IOException;
import textsock.TextSocket;

// Author: Jun Cai
public class WebClient {
    public static void main(String[] args) throws IOException {
        int n = Integer.parseInt(args[0]);
        TextSocket conn = new TextSocket("localhost", 10002);
        conn.putln(n + "");
        conn.putln("");

        for (String line : conn) {
            System.out.println(line);
        }

        conn.close();
    }
}

