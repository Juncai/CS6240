package edu.neu.jon.JavaThreading;

/**
 * Created by jonca on 1/15/2016.
 */
public class MyThread extends Thread {
    private String name;

    public MyThread (String name) {
        this.name = name;
    }

    public void run() {
        System.out.format("Hello from %s!%n", this.name);
    }

    public static void main(String[] args) throws InterruptedException {
        MyThread t1 = new MyThread("One");
        MyThread t2 = new MyThread("Two");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

    }

}
