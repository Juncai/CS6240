/*
    Compile:
        javac Factorize.java

    Usage:
        java Factorize NUMBER_TO_BE_FACTORIZED

    Setting threads:
        create a environment variable named "THREADS", set the value to be 1 or 2

    Note:
        This code works for both 1 and 2 threads. And achieved better performance with two threads.
        Performance reference:
            Single thread with input 32281802098926944263: 91 sec
            Two threads with input 32281802098926944263: 46 sec

        Changes on the original code:
            * Split the work for factors larger than 2 on two separate threads
                Thread 1 for factors started at 3 and incremented by 4 every iteration
                Thread 2 for factors started at 5 and incremented by 4 every iteration
            * Share a minNN as a global variable among all the threads
                minNN stores the minimal nn across all the threads at that point
                nn and minNN are updated as follow:
                    if the thread's current nn is larger than minNN, set nn = minNN and update rn
                    if the thread's current nn is smaller than minNN, update minNN = nn
 */

import java.math.BigInteger;
import java.util.ArrayList;

public class Factorize {
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
            factors = factorizeWithTwoThreads(nn);
        }
        return factors;
    }

    public static ArrayList<BigInteger> factorizeWithTwoThreads(BigInteger nn) throws InterruptedException {
        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();

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

        // check and remove redundant factor in the two threads
        BigInteger lastOfT1 = t1.factors.get(t1.factors.size() - 1);
        BigInteger lastOfT2 = t2.factors.get(t2.factors.size() - 1);

        if (lastOfT1.equals(lastOfT2)) {
            t1.factors.remove(lastOfT1);
        }
        factors.addAll(t1.factors);
        factors.addAll(t2.factors);

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
    static BigInteger minNN = null;
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

        if (minNN == null) {
            minNN = new BigInteger(nn.toString());
        }
    }

    public void run() {
        this.factors = factorizeHelper(this.nn, this.rn, this.init, this.interval);
    }

    private ArrayList<BigInteger> factorizeHelper(BigInteger nn, BigInteger rn, BigInteger ii, BigInteger interval) {
        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();

        while (ii.compareTo(rn) <= 0) {
            if (nn.mod(ii).equals(BigMath.ZERO)) {
                factors.add(ii);
                nn = nn.divide(ii);
                rn = BigMath.sqrt(nn);
                // update minNN
                if (nn.compareTo(minNN) < 0) {
                    minNN = new BigInteger(nn.toString());
                }
            }
            else {
                ii = ii.add(interval);
            }

            // check if minNN is smaller than nn
            if (nn.compareTo(minNN) > 0) {
                nn = new BigInteger(minNN.toString());
                rn = BigMath.sqrt(nn);
            }
        }

        factors.add(nn);

        return factors;
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