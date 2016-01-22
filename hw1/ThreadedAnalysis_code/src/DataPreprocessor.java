import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class DataPreprocessor {

    public static List<String[]> unzipAndParseCSV(String gzPath) {
        List<String[]> records;

        try {
            FileInputStream fis = new FileInputStream(gzPath);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            InputStreamReader isr = new InputStreamReader(gzis);

            records = getCSVRecords(isr);
        } catch (IOException ex) {
            //  ex.printStackTrace();
            return null;
        }
        return records;
    }


    public static List<String[]> getCSVRecords(Reader reader) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        List<String[]> records = new List<String[]>();

        br.readLine();  // get rid of the header
        String line;
        String[] values;
        while ((line = br.readLine()) != null) {
            numOfFlights++;
            values = parseCSVLine(line);
            records.add(values);
        }

        br.close();

        return records;
    }

    /**
     * Parse a line in CSV format
     * @param line a string in CSV format
     * @return list of values as a string array
     */
    public static String[] parseCSVLine(String line) {
        ArrayList<String> values = new ArrayList<>();
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
}