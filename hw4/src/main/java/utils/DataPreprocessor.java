package utils;

import org.apache.commons.math3.linear.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.*;



public class DataPreprocessor {


    public static boolean processLine(String line, Text carrier,
                                      List<Double[][]> dLOM, List<Double[][]> tLOM,
                                      double[] stats)
            throws IOException {
        double[][] xd = new double[1][2];
        double[][] xt = new double[1][2];
        double[][] y = new double[1][1];
        String date;
        int year;

        dLOM.add(newMatrix(3, 3));
        dLOM.add(newMatrix(3, 1));
        tLOM.add(newMatrix(3, 3));
        tLOM.add(newMatrix(3, 1));

        String[] values;
        values = parseCSVLine(line);
        if (!sanityCheck(values)) {
            carrier.set(OTPConsts.INVALID);
        } else {
            carrier.set(values[OTPConsts.UNIQUE_CARRIER]);
            date = values[OTPConsts.FL_DATE];
            year = getYear(date);
//            System.out.println(year);
            if (year == OTPConsts.ACTIVE_YEAR) {
                return true;
            }

            if (year >= OTPConsts.TARGET_YEAR_START && year <= OTPConsts.TARGET_YEAR_END) {
                y[0][0] = Double.parseDouble(values[OTPConsts.AVG_TICKET_PRICE]);
                xd[0][0] = (Double.parseDouble(values[OTPConsts.DISTANCE]) - stats[0])
                        / stats[2];
                xd[0][1] = 1.0;
                xt[0][0] = (Double.parseDouble(values[OTPConsts.AIR_TIME]) - stats[1])
                        / stats[3];
                xt[0][1] = 1.0;
                dLOM.clear();
                dLOM.add(dToD(getXTY(xd, xd)));
                dLOM.add(dToD(getXTY(xd, y)));
                tLOM.clear();
                tLOM.add(dToD(getXTY(xt, xt)));
                tLOM.add(dToD(getXTY(xt, y)));
            }
        }
        return false;
    }
//
//     public static void resetMatrices(List<Double[][]> lom) {
//        for (int i = 0; i < 2; i++) {
//            for (int j = 0; j < 2; j++) {
//                lom.get(0)[i][j] = 0.0;
//                lom.get(1)[i][j] = 0.0;
//            }
//        }
//    }
//
//    public static void replaceMatrices(List<Double[][]> lom, double[][] m1, double[][] m2) {
//        for (int i = 0; i < 2; i++) {
//            for (int j = 0; j < 2; j++) {
//                lom.get(0)[i][j] = m1[i][j];
//                lom.get(1)[i][j] = m2[i][j];
//            }
//        }
//    }

    public static void updateMatrices(List<Double[][]> lom1, List<Double[][]> lom2) {
        for (int h = 0; h < lom1.size(); h++) {
            for (int i = 0; i < lom1.get(h).length; i++) {
                for (int j = 0; j < lom1.get(h)[0].length; j++) {
                    lom1.get(h)[i][j] += lom2.get(h)[i][j];
                }
            }
        }
    }

    public static List<Double[][]> getNewLOM() {
        List<Double[][]> res = new ArrayList<Double[][]>();
        res.add(newMatrix(2, 2));
        res.add(newMatrix(2, 1));
        return res;
    }

    public static Double[][] newMatrix(int r, int c) {
        Double[][] res = new Double[r][c];
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                res[i][j] = .0;
            }
        }
        return res;
    }

    public static void initLOM(List<Double[][]> lom) {
        for (int h = 0; h < lom.size(); h++) {
            for (int i = 0; i < lom.get(h).length; i++) {
                for (int j = 0; j < lom.get(h)[0].length; j++) {
                    lom.get(h)[i][j] = .0;
                }
            }
        }
    }

   /**
     * Parse a line in CSV format
     * @param line a string in CSV format
     * @return list of values as a string array
     */
    static String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        boolean inQuote = false;
        char curChar;
        for (int i = 0; i < line.length(); i++) {
            curChar = line.charAt(i);
            if (inQuote) {
                if (curChar == '"') {
                    inQuote = false;
                } else {
                    sb.append(curChar);
                }

            } else {
                if (curChar == '"') {
                    inQuote = true;
                } else if (curChar == ',') {
                    values.add(sb.toString());
                    sb = new StringBuffer();
                } else {
                    sb.append(curChar);
                }
            }
        }
        values.add(sb.toString());  // last field

        return values.toArray(new String[1]);
    }

    /**
     * Check the number of fields in the record and the logic between some fields
     * @param values a String array contains the value of each field in a record
     * @return true if the input is a valid record, false otherwise
     */
    static boolean sanityCheck(String[] values) {
        if (values.length != 110) return false;
        try {
            // check not 0
            for (int i : OTPConsts.NOTZERO) {
                if (Double.parseDouble(values[i]) == 0) return false;
            }

            double timeZone = getMinDiff(values[OTPConsts.CRS_ARR_TIME], values[OTPConsts.CRS_DEP_TIME])
                    - Double.parseDouble(values[OTPConsts.CRS_ELAPSED_TIME]);
            double residue = timeZone % 60;
            if (residue != 0) return false;

            // check larger than 0
            for (int i : OTPConsts.LARGERTHANZERO) {
                if (Double.parseDouble(values[i]) <= 0) return false;
            }

            // check not empty
            for (int i : OTPConsts.NOTEMPTY) {
                if (values[i].isEmpty()) return false;
            }

            // for flights not canceled
            boolean isCanceled = (Double.parseDouble(values[OTPConsts.CANCELLED]) == 1);

            // ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
            if (!isCanceled) {
                double timeDiff = getMinDiff(values[OTPConsts.ARR_TIME], values[OTPConsts.DEP_TIME])
                        - Double.parseDouble(values[OTPConsts.ACTUAL_ELAPSED_TIME]) - timeZone;
                if (timeDiff != 0) return false;

                double arrDelay = Double.parseDouble(values[OTPConsts.ARR_DELAY]);
                double arrDelayNew = Double.parseDouble(values[OTPConsts.ARR_DELAY_NEW]);
                // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
                if (arrDelay > 0) {
                    if (arrDelay != arrDelayNew) return false;
                }

                // if ArrDelay < 0 then ArrDelayMinutes???? should be zero
                if (arrDelay < 0) {
                    if (arrDelayNew != 0) return false;
                }
                // if ArrDelayMinutes >= 15 then ArrDel15 should be false
                boolean arrDel15 = (Double.parseDouble(values[OTPConsts.ARR_DEL15]) == 1);
                if (arrDelayNew >= 15 && !arrDel15) return false;
            }

            // finally, check the carrier field and price field
            String carrier = values[OTPConsts.UNIQUE_CARRIER];
            if (carrier.isEmpty()) return false;
            double avgTicketPrice = Double.parseDouble(values[OTPConsts.AVG_TICKET_PRICE]);
            String airTime = values[OTPConsts.AIR_TIME];
            double airTimeVal = Double.parseDouble(airTime);
            double distance = Double.parseDouble(values[OTPConsts.DISTANCE]);

        } catch (Exception ex) {
            // ex.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Calculate the difference between two time stamp in minutes.
     * Note that the first timestamp is always later than the second one.
     * @param t1 first timestamp in "HHmm" format
     * @param t2 second timestamp in "HHmm" format
     * @return difference between t1 and t2 in minutes
     */
    static int getMinDiff(String t1, String t2) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("HHmm");
            Date date1 = format.parse(t1);
            Date date2 = format.parse(t2);
            long timeDiff = (date1.getTime() - date2.getTime()) / 60000;
            if (timeDiff <= 0) {
                timeDiff += 24 * 60;
            }

            return (int) timeDiff;
        } catch (ParseException ex) {
            return -1;
        }
    }

    /**
     * Get mean of ticket price for each eligible carrier.
     * @return a map contains carrier mean price pairs.
     */
    public static double getMean(List<Double> prices) {
        double sum = sumOfDoubleList(prices);
        return sum / prices.size();
    }

    /**
     * Compute sum of all the elements in a double list.
     * @param lod a list of double instances
     * @return sum of all the doubles in the list
     */
    public static double sumOfDoubleList(List<Double> lod) {
        double sum = 0;
        for (double d : lod){
            sum += d;
        }
        return sum;
    }

    /**
     * Get median from a sorted list of doubles
     * @param lod a sorted list of doubles
     * @return the median
     */
    public static double getMedian(List<Double> lod) {
        Collections.sort(lod);
        int len = lod.size();
        if (len % 2 == 0) {
            return (lod.get(len / 2) + lod.get((len / 2) - 1)) / 2;
        } else {
            return lod.get(len / 2);
        }
    }

    public static int getYear(String v) {
        return Integer.parseInt(v.split("-")[0]);
    }

    public static String getMonth(Text value) {
        String[] dates =  value.toString().split(" ")[0].split("-");
        return dates[0] + "-" + dates[1];
    }

    public static double getPrice(Text value) {
        return Double.parseDouble(value.toString().split(" ")[1]);
    }

//    public static double[][] getXTY(double[][] x, double[][] y) {
//        double[][] xt = transpose(x);
//        return matrixMultiply(xt, y);
//    }

   public static double[][] getXTY(double[][] x, double[][] y) {
        double[][] xt = transpose(x);
        return matrixMultiply(xt, y);
    }

    public static Double[][] dToD(double[][] x) {
        Double[][] res = new Double[x.length][x[0].length];
        for (int i = 0; i < x.length; i++) {
            for (int j = 0; j < x[0].length; j++) {
                res[i][j] = x[i][j];
            }
        }
        return res;
    }

    public static double[][] dFromD(Double[][] x) {
        double[][] res = new double[x.length][x[0].length];
        for (int i = 0; i < x.length; i++) {
            for (int j = 0; j < x[0].length; j++) {
                res[i][j] = x[i][j];
            }
        }
        return res;
    }

   public static double[][] matrixMultiply(double[][] a, double[][] b) {
        double[][] res = new double[a.length][b[0].length];
        double ijVal;
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < b[0].length; j++) {
                ijVal = 0;
                for (int k = 0; k < a[0].length; k++) {
                    ijVal += a[i][k] * b[k][j];
                }
                res[i][j] = ijVal;
            }
        }
        return res;
    }

    public static double[][] transpose(double[][] x) {
        double[][] res = new double[x[0].length][x.length];
        for (int i = 0; i < x.length; i++) {
            for (int j = 0; j < x[0].length; j++) {
                res[j][i] = x[i][j];
            }
        }
        return res;
    }

    public static String serializeMatrices(List<Double[][]> lom) {
        StringBuilder sb = new StringBuilder();
        for (int h = 0; h < lom.size(); h++) {
            for (int i = 0; i < lom.get(h).length; i++) {
                for (int j = 0; j < lom.get(h)[0].length; j++) {
                    sb.append(lom.get(h)[i][j]);
                    sb.append(',');
                }
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String serializeMatrix(double[][] m) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < m[0].length; j++) {
                sb.append(m[i][j]);
                sb.append(',');
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static List<Double[][]> deserializeMatrices(String str) {
        List<Double[][]> lom = new ArrayList<Double[][]>();
        lom.add(newMatrix(2, 2));
        lom.add(newMatrix(2, 1));
		if (!str.equals(OTPConsts.ACTIVE)) {
			String[] strs = str.split(",");
			for (int i = 0; i < 2; i++) {
				for (int j = 0; j < 2; j++) {
                    lom.get(0)[i][j] = Double.parseDouble(strs[2 * i + j]);
				}
			}
            for (int i = 0; i < 2; i++) {
                lom.get(1)[i][0] = Double.parseDouble(strs[4 + i]);
            }
		}
        return lom;
    }

    public static boolean isActiveCarrier(Iterable<Text> values) {
        for (Text t : values) {
            if (t.toString().equals(OTPConsts.ACTIVE)) return true;
        }
        return false;
    }

//    public static double[][] fit(double[][] xtx, double[][] xty) {
//        // first calculate xtx^-1
//        double[][] xtx_inv = inverse(xtx);
//        double[][] theta = matrixMultiply(xtx_inv, xty);
//        return theta;
//    }

    public static double[][] fit(List<Double[][]> lom) {
        // first calculate xtx^-1
        double[][] xtx = dFromD(lom.get(0));
        double[][] xty = dFromD(lom.get(1));
        double[][] xtx_inv = inverse(xtx);
        double[][] theta = matrixMultiply(xtx_inv, xty);
        return theta;
    }

    public static double[][] inverse(double[][] m) {
        RealMatrix rm = new Array2DRowRealMatrix(m);
        DecompositionSolver ds = new LUDecomposition(rm).getSolver();
        if (ds.isNonSingular()) {
            RealMatrix inv = ds.getInverse();
            return inv.getData();
        }
        return new double[2][1];
    }

    public static double[] getStats(Path p) {
        double[] res = {0, 0, 1, 1};
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(p)));
            String line =br.readLine();
            res[0] = Double.parseDouble(line.split(",")[0]);
            res[1] = Double.parseDouble(line.split(",")[1]);
            line=br.readLine();
            res[2] = Double.parseDouble(line.split(",")[0]);
            res[3] = Double.parseDouble(line.split(",")[1]);
        } catch (Exception e) {
        }
        return res;
    }

}
