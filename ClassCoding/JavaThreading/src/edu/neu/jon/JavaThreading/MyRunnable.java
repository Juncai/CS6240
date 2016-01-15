package edu.neu.jon.JavaThreading;

/**
 * Created by jonca on 1/15/2016.
 */
public class MyRunnable implements Runnable {
    private String name;

    public MyRunnable(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        System.out.format("This is MyRunnable %s%n", this.name);
    }

    public static void main(String[] args) {
        MyRunnable r1 = new MyRunnable("one");
        MyRunnable r2 = new MyRunnable("two");

        r1.run();
        r2.run();
    }
}
