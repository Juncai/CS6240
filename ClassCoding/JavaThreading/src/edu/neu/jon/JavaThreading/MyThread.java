package edu.neu.jon.JavaThreading;

/**
 * Created by jonca on 1/15/2016.
 */
public class MyThread extends Thread {
    private String name;
    private static int counter = 0;
    private int num;

    public MyThread (String name, int num) {
        this.name = name;
        this.num = num;
    }

    public void run() {
        System.out.format("Hello from %s!%n", this.name);
        for (int i = 0; i < num; i++) {
            counter++;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int numOfAdd = 9000;
        MyThread t1 = new MyThread("One", numOfAdd);
        MyThread t2 = new MyThread("Two", numOfAdd);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        System.out.format("The final value of the counter is %d%n", counter);
    }

}