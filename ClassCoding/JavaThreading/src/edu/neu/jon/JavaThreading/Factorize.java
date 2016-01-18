package edu.neu.jon.JavaThreading;

import java.math.BigInteger;
import java.util.ArrayList;

public class Factorize {
    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("Usage: java Factorize 123456");
        }
        BigInteger nn = new BigInteger(args[0]);

        WorkThread t1 = new WorkThread(nn, true);
        WorkThread t2 = new WorkThread(nn, false);

        t1.run();
        t2.run();

        t1.join();
        t2.join();

        ArrayList<BigInteger> factors = new ArrayList<>();
        factors.addAll(t1.factors);
        factors.addAll(t2.factors);

//        ArrayList<BigInteger> factors = factorize(nn);

        System.out.println("Factors:");
        for (BigInteger xx : factors) {
            System.out.println(xx);
        }

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000);
    }

    static ArrayList<BigInteger> factorize(BigInteger nn) {
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

    public static ArrayList<BigInteger> factorizeByTwo(BigInteger nn) {
        BigInteger rn = BigMath.sqrt(nn);

        System.out.println("Input: " + nn);
        System.out.println("Sqrt: "  + rn);

        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();

        while (nn.mod(BigMath.TWO).equals(BigMath.ZERO)) {
            factors.add(BigMath.TWO);
            nn = nn.divide(BigMath.TWO);
            rn = BigMath.sqrt(nn);
        }

        if (nn.compareTo(new BigInteger("2")) > 0) {
            factors.add(nn);
        }

        return factors;
    }

    public static ArrayList<BigInteger> factorizeByThreeAndAbove(BigInteger nn) {
//        BigInteger rn = BigMath.sqrt(nn);
        BigInteger rn = nn.divide(BigMath.TWO).add(BigInteger.ONE);

        System.out.println("Input: " + nn);
        System.out.println("Sqrt: "  + rn);

        ArrayList<BigInteger> factors = new ArrayList<BigInteger>();
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
        if (nn.compareTo(new BigInteger("3")) > 0) {
            factors.add(nn);
        }

        return factors;
    }


}

class WorkThread extends Thread {
    private boolean isFactorizeByTwo;
    private BigInteger nn;
    public ArrayList<BigInteger> factors;

    public WorkThread (BigInteger nn, boolean isFactorizeByTwo) {
        this.isFactorizeByTwo = isFactorizeByTwo;
        this.nn = nn;
    }

    public void run() {
        if (this.isFactorizeByTwo) {
            this.factors = Factorize.factorizeByTwo(this.nn);
        } else {
            this.factors = Factorize.factorizeByThreeAndAbove(this.nn);
        }

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
