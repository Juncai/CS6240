package edu.neu.jon.JavaThreading;

import java.math.BigInteger;
import java.util.ArrayList;

public class Factorize {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("Usage: java Factor 123456");
        }

        BigInteger nn = new BigInteger(args[0]);
        ArrayList<BigInteger> factors = factorize(nn);

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
}

