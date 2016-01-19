package edu.neu.jon.JavaThreading;

import java.math.BigInteger;
import java.util.ArrayList;

public class FactorizeMT {
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: java Factor 123456");
        }

        long startTime = System.currentTimeMillis();

        int numOfThreads = 1;
        String threadsEnv = System.getenv("THREADS");
        if (threadsEnv != null) {
            numOfThreads = Integer.parseInt(threadsEnv);
            if (numOfThreads < 1 || numOfThreads > 2) {
                System.out.println("The valid number of threads is 1 or 2!");
                return;
            }
        }
        System.out.format("Running with %d thread(s)!%n", numOfThreads);

        BigInteger nn = new BigInteger(args[0]);
        ArrayList<BigInteger> factors = factorize(nn, numOfThreads);

        System.out.println("Factors:");
        for (BigInteger xx : factors) {
            System.out.println(xx);
        }
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000);
    }

    public static ArrayList<BigInteger> factorize(BigInteger nn) throws InterruptedException {
        return factorize(nn, 1);
    }


    public static ArrayList<BigInteger> factorize(BigInteger nn, int t) throws InterruptedException {
        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();
        if (t == 1) {
            factors = factorizeSingle(nn);
        }
        if (t == 2) {
            BigInteger[] newNNAndRN = factorizeWithTwo(nn, factors);
            BigInteger newNN = newNNAndRN[0];
            BigInteger rn = newNNAndRN[1];

            BigInteger start1 = new BigInteger("3");
            BigInteger start2 = new BigInteger("5");
            BigInteger interval = new BigInteger("4");

            WorkThread t1 = new WorkThread(newNN, rn, start1, interval);
            WorkThread t2 = new WorkThread(newNN, rn, start2, interval);

            t1.run();
            t2.run();

            t1.join();
            t2.join();

            factors.addAll(t1.factors);
            factors.addAll(t2.factors);
        }

        return factors;
    }

    static ArrayList<BigInteger> factorizeHelper(BigInteger nn, BigInteger rn, BigInteger ii, BigInteger interval) {
        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();

        while (ii.compareTo(rn) <= 0) {
            if (nn.mod(ii).equals(BigMath.ZERO)) {
                factors.add(ii);
                nn = nn.divide(ii);
                rn = BigMath.sqrt(nn);
            }
            else {
                ii = ii.add(interval);
            }
        }

        factors.add(nn);

        return factors;
    }


    static BigInteger[] factorizeWithTwo(BigInteger nn, ArrayList<BigInteger> factors) {
        BigInteger rn = BigMath.sqrt(nn);

        System.out.println("Input: " + nn);
        System.out.println("Sqrt: "  + rn);


        while (nn.mod(BigMath.TWO).equals(BigMath.ZERO)) {
            factors.add(BigMath.TWO);
            nn = nn.divide(BigMath.TWO);
            rn = BigMath.sqrt(nn);
        }

        return new BigInteger[]{nn, rn};
    }


    static ArrayList<BigInteger> factorizeSingle(BigInteger nn) {
        BigInteger rn = BigMath.sqrt(nn);

        System.out.println("Input: " + nn);
        System.out.println("Sqrt: "  + rn);

        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();

        while (nn.mod(BigMath.TWO).equals(BigMath.ZERO)) {
            factors.add(BigMath.TWO);
            nn = nn.divide(BigMath.TWO);
            rn = BigMath.sqrt(nn);
        }

        BigInteger ii = new BigInteger("3");
        while (ii.compareTo(rn) <= 0) {
            if (nn.mod(ii).equals(BigMath.ZERO)) {
                factors.add(ii);
                nn = nn.divide(ii);
                rn = BigMath.sqrt(nn);
            }
            else {
                ii = ii.add(BigMath.TWO);
            }
        }

        factors.add(nn);

        return factors;
    }
}

class WorkThread extends Thread {
    private BigInteger nn;
    private BigInteger rn;
    private BigInteger init;
    private BigInteger interval;
    public ArrayList<BigInteger> factors;

    public WorkThread (BigInteger nn, BigInteger rn, BigInteger init, BigInteger interval) {
        this.nn = new BigInteger(nn.toString());
        this.rn = new BigInteger(rn.toString());
        this.init = new BigInteger(init.toString());
        this.interval = new BigInteger(interval.toString());
    }

    public void run() {
        this.factors = FactorizeMT.factorizeHelper(this.nn, this.rn, this.init, this.interval);
    }
}


class BigMath {
    public final static BigInteger TWO  = new BigInteger("2");
    public final static BigInteger ZERO = new BigInteger("0");

    static BigInteger sqrt(BigInteger nn) {
        return sqrtSearch(nn, TWO, nn);
    }

    static BigInteger sqrtSearch(BigInteger nn, BigInteger lo, BigInteger hi) {
        BigInteger xx = lo.add(hi).divide(TWO);

        if (xx.equals(lo) || xx.equals(hi)) {
            return xx;
        }

        BigInteger dy = nn.subtract(xx.multiply(xx));
        if (dy.compareTo(ZERO) < 0) {
            return sqrtSearch(nn, lo, xx);
        }
        else {
            return sqrtSearch(nn, xx, hi);
        }
    }
}