package analysis;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class DataPreprocessor {
    private long k;
    private long f;
    private Map<String, CarrierData> resMap;

    public DataPreprocessor(Map<String, CarrierData> carrierMap) {
        this.k = 0;
        this.f = 0;
        this.resMap = carrierMap;
    }

    public long getK() {
        return this.k;
    }

    public long getF() {
        return this.f;
    }

    public void unzipAndParseCSV(String gzPath) {

        try {
            FileInputStream fis = new FileInputStream(gzPath);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            InputStreamReader isr = new InputStreamReader(gzis);

            getCSVRecords(isr);
        } catch (IOException ex) {
            return;
        }
    }

    /**
     * Get records from the given reader, then add to result map.
     * @param reader Reader object
     * @throws IOException
     */
    public void getCSVRecords(Reader reader) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        String carrier;
        String month;
        Double price;
        CarrierData newCarrier;

        br.readLine();  // get rid of the header
        String line;
        String[] values;
        while ((line = br.readLine()) != null) {
            values = parseCSVLine(line);
            if (!sanityCheck(values)) {
                this.k++;
            } else {
                this.f++;
                carrier = values[OTPConsts.UNIQUE_CARRIER];
                price = Double.parseDouble(values[OTPConsts.AVG_TICKET_PRICE]);
                month = getMonthFromStr(values[OTPConsts.FL_DATE]);
                if (!this.resMap.containsKey(carrier)) {
                    newCarrier = new CarrierData(carrier);
                    this.resMap.put(carrier, newCarrier);
                }
                this.resMap.get(carrier).addRecord(month, price);
            }
        }

        br.close();
    }

    public String getMonthFromStr(String date) {
        String[] nums = date.split("-");
        return nums[0] + "-" + nums[1];
    }

    /**
     * Parse a line in CSV format
     * @param line a string in CSV format
     * @return list of values as a string array
     */
    public String[] parseCSVLine(String line) {
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
    public boolean sanityCheck(String[] values) {
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
    public int getMinDiff(String t1, String t2) {
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
}