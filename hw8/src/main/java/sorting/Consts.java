package sorting;

/**
 * Created by jon on 3/23/16.
 */
public class Consts {
    public static final String END_OF_LINE = "\r\n";
    public static final String END_OF_DATA = "EOD";
    public static final String HEADER_START = "Wban Number,";
    public static final String NODE_READY = "READY";
    public static final String COMMA = ",";
    public enum Stage {
        SAMPLE, SELECT, SORT
    }
    public static final int SAMPLE_FREQUENCY = 500;

    // INDEX
    public static final int WBAN_NUMBER = 0;
    public static final int DATE = 1;
    public static final int TIME = 2;
    public static final int DRY_BULB_TEMP = 8;

    // sample rate
    public static final int SAMPLE_BASE = 100;
}
