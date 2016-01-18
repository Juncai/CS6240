import java.math.BigInteger;
import java.util.ArrayList;

public class FactorizeOriginal {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Factor 123456");
        }

        BigInteger nn = new BigInteger(args[0]);
        ArrayList<BigInteger> factors = factorize(nn);

        System.out.println("Factors:");
        for (BigInteger xx : factors) {
            System.out.println(xx);
        }
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
