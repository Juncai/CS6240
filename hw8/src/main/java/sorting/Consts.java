package sorting;

// Author: Jun Cai
public class Consts {
    public static final String END_OF_LINE = "\r\n";
    public static final String END_OF_LINE_L = "\n";
    public static final String END_OF_DATA = "EOD";
    public static final String END_OF_DATA_EOL = END_OF_DATA + END_OF_LINE;
    public static final String HEADER_START = "Wban Number,";
    public static final String NODE_READY = "READY";
    public static final String NODE_READY_EOL = NODE_READY + END_OF_LINE;
    public static final String COMMA = ",";
    public static final String DELIMITER = ", ";
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
    public static final int SAMPLE_BASE = 50;

    // Communication headers
    public static final String MASTER_HEADER = "MASTER";
    public static final String SAMPLE_HEADER = "SAMPLE";
    public static final String DATA_HEADER = "DATA";
    public static final String READY_HEADER = "READY";

    // master request
    public static final String STATUS_REQ = "STATUS";
    public static final String SHUTDOWN_REQ = "SHUTDOWN";


    // data buffer file prefix
    public static final String BUFFER_FILE_PREFIX = "/dev/shm/buffer_";


}
