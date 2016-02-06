package analysis;

import java.util.*;

/**
 * Created by jon on 2/2/16.
 */
public class CarrierData {
    public String id;
    public Map<String, List<Double>> monthPrices;
    public boolean isActive;
    private int totalFlights;
    public Map<String, Double> means;
    public Map<String, Double> medians;

    public int getTotalFlights() {
        return totalFlights;
    }

    public CarrierData(String id) {
        this.id = id;
        monthPrices = new TreeMap<String, List<Double>>();
        isActive = false;
        totalFlights = 0;
    }

    public void addRecord(String month, Double price) {
        totalFlights++;

        if (month.startsWith(OTPConsts.ACTIVE_YEAR)) isActive = true;

        List<Double> prices;
        if (!monthPrices.containsKey(month)) {
            prices = new ArrayList<Double>();
            prices.add(price);
            monthPrices.put(month, prices);
        } else {
            monthPrices.get(month).add(price);
        }
    }

    public void combine(CarrierData data) {
        for (String m : data.monthPrices.keySet()) {
            for (double p : data.monthPrices.get(m)) {
                addRecord(m, p);
            }
        }
    }
}
