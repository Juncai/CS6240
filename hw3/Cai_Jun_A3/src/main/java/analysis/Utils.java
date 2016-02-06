package analysis;

import java.util.*;
import java.lang.*;

/**
 * Created by jon on 2/2/16.
 */
public class Utils {


    public static List<CarrierData> getTopCarriers(int n, Map<String, CarrierData> map) {
//        List<CarrierData> allC = Arrays.asList((CarrierData[])map.values().toArray());
        List<CarrierData> allC = new ArrayList<CarrierData>();
        for (CarrierData cd : map.values()) {
            allC.add(cd);
        }

        Comparator<CarrierData> comp = new Comparator<CarrierData>() {
            @Override
            public int compare(CarrierData c1, CarrierData c2) {
                if (c1.getTotalFlights() > c2.getTotalFlights()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        };
        Collections.sort(allC, comp);

        // if there isn't enough carriers, just return the sorted list;
        if (n > allC.size()) return allC;

        List<CarrierData> res = new ArrayList<CarrierData>();
        for (int i = 0; i < n; i ++ ) {
            res.add(allC.get(i));
        }
        return res;
    }

    public static void getMeanForCarrier(CarrierData c) {
        Map<String, Double> means = getSum(c.monthPrices);
        for (String k : means.keySet()) {
            means.put(k, means.get(k) / c.monthPrices.get(k).size());
        }
        c.means = means;
    }

    public static void getMedianForCarrier(CarrierData c, boolean fast) {
        Map<String, Double> medians = new HashMap<String, Double>();
        double median;
        for (String k : c.monthPrices.keySet()) {
            if (fast) {
//                median = fastMedianTwo(c.monthPrices.get(k));
                median = fastMedianOne(c.monthPrices.get(k));
            } else {
                median = median(c.monthPrices.get(k));
            }
            medians.put(k, median);
        }
        c.medians = medians;
    }

    public static double median(List<Double> lod) {
        Collections.sort(lod);
        int len = lod.size();
        if (len % 2 == 0) {
            return (lod.get(len / 2) + lod.get((len / 2) - 1)) / 2;
        } else {
            return lod.get(len / 2);
        }
    }

    public static double fastMedianTwo(List<Double> lod) {
        // implement the median of medians
        // Pseudo code reference: https://en.wikipedia.org/wiki/Median_of_medians
        int medianIndex = MOMSelect(lod, 0, lod.size() - 1, lod.size() / 2);
        return lod.get(medianIndex);
    }

    public static int MOMSelect(List<Double> list, int left, int right, int n) {
        if (left == right) return left;
        int pIndex;

        while (true) {
            pIndex = MOMPivot(list, left, right);
            pIndex = MOMPartition(list, left, right, pIndex);
            if (n == pIndex) {
                return n;
            } else if (n < pIndex) {
                right = pIndex - 1;
            } else {
                left = pIndex + 1;
            }
        }
    }

    public static int MOMPivot(List<Double> list, int left, int right) {
        if (right - left < 5) {
            return partition5(list, left, right);
        }

        for (int i = left; i <= right; i += 5) {
            int subRight = i + 4;
            subRight = (subRight > right) ? right : subRight;
            int median5 = partition5(list, i, subRight);
            swap(list, median5, left + (i - left) / 5);
        }
        return MOMSelect(list, left, left + (int)Math.ceil(1.0 * (right - left) / 5), left + (right - left) / 10);
    }

    public static int partition5(List<Double> list, int left, int right) {
        List<Double> subList = new ArrayList<Double>();
        // TODO < or <= right?
        for (int i = left; i <= right; i++) {
            subList.add(list.get(i));
        }
        Collections.sort(subList);
        for (int i = 0; i < subList.size(); i++) {
            list.set(left + i, subList.get(i));
        }
        return subList.size() / 2;
    }

    public static int MOMPartition(List<Double> list, int left, int right, int pIndex) {
        double pValue = list.get(pIndex);
        swap(list, left, right);
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (list.get(i) < pValue) {
                swap(list, storeIndex, i);
                storeIndex++;
            }
        }
        swap(list, right, storeIndex);
        return storeIndex;
    }

    public static void swap(List<Double> list, int i1, int i2) {
        double tmp = list.get(i1);
        list.set(i1, list.get(i2));
        list.set(i2, tmp);
    }

    public static double fastMedianOne(List<Double> lod) {
        // need to implement
        // A naive approach which only uses 10 percent of the data to find the median
        // need to randomize
        int len = lod.size();
        List<Double> subLod = lod;

        if (len > 50) {
            subLod = new ArrayList<Double>();

            Random random = new Random();
            int rIndex;

            for (int i = 0; i < len / 10; i++) {
                rIndex = random.nextInt(len);
                subLod.add(lod.get(rIndex));
            }
        }
        return median(subLod);
    }

    public static Map<String, Double> getSum(Map<String, List<Double>> map) {
        Map<String, Double> sumMap = new HashMap<String, Double>();
        for (String k : map.keySet()) {
            sumMap.put(k, sumOfDoubleList(map.get(k)));
        }
        return sumMap;
    }

    public static double sumOfDoubleList(List<Double> lod) {
        double sum = 0;
        for (double d : lod){
            sum += d;
        }
        return sum;
    }

//
//    public static void main(String[] argv) {
//        List<Double> lod = Arrays.asList(3.0, 5.0, 2.0, 1.0, 5.0, 4.0, 7.0, 8.0);
//        double m1 = 0;
//        double m2 = 0;
//        double m3 = 0;
//        m1 = median(lod);
//        m2 = fastMedianTwo(lod);
////        m3 = fastMedianOne(lod);
//        System.out.printf("Normal median: %f, MOM: %f, Stupid median: %f\n", m1, m2, m3);
//    }
}
