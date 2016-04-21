package job.assignment5.utils;


// Authors: Jun Cai and Vikas Boddu
public class OTPConsts {
    // 2400
    public final static String START_OF_NEW_DAY_OLD = "2400";
    public final static String START_OF_NEW_DAY = "0000";

    // Header start with
    public final static String HEADER_START = "YEAR";

    // Date time format
    public final static String DATETIME_FORMAT = "yyyy-MM-dd HHmm";
    public final static String DATEKEY_FORMAT = "yyyyMMddHH";

    // for flight time and distance
    public final static int DISTANCE = 54;
    public final static int AIR_TIME = 52;

    // constants for indices
    public final static int YEAR = 0;
    public final static int FL_DATE = 5;
    public final static int CRS_ARR_TIME = 40;
    public final static int CRS_DEP_TIME = 29;
    public final static int CRS_ELAPSED_TIME = 50;
    public final static int[] NOTZERO = {29, 40};

    // ORIGIN_AIRPORT_ID 11
    public final static int ORIGIN_AIRPORT_ID = 11;
    // DEST_AIRPORT_ID 20 (do we need DIV_AIRPORT_ID?)
    public final static int DEST_AIRPORT_ID = 20;


    // ORIGIN_AIRPORT_ID 11
    // ORIGIN_AIRPORT_SEQ_ID 12
    // ORIGIN_CITY_MARKET_ID 13
    // ORIGIN_STATE_FIPS 17
    // ORIGIN_WAC 19
    // DEST_AIRPORT_ID 20 (do we need DIV_AIRPORT_ID?)
    // DEST_AIRPORT_SEQ_ID 21
    // DEST_CITY_MARKET_ID 22
    // DEST_STATE_FIPS 26
    // DEST_WAC 28
    public final static int[] LARGERTHANZERO = {11, 12, 13, 17, 19, 20, 21, 22, 26, 28};

    // ORIGIN 14
    // ORIGIN_CITY_NAME 15
    // ORIGIN_STATE_ABR 16
    // ORIGIN_STATE_NM 18
    // DEST 23
    // DEST_CITY_NAME 24
    // DEST_STATE_ABR 25
    // DEST_STATE_NM 27
    public final static int[] NOTEMPTY = {14, 15, 16, 18, 23, 24, 25, 27};

    public final static int CANCELLED = 47;
    public final static int DEP_TIME = 30;
    public final static int ARR_TIME = 41;
    public final static int ACTUAL_ELAPSED_TIME = 51;
    public final static int ARR_DELAY = 42;
    public final static int DEP_DELAY = 31;
    public final static int ARR_DELAY_NEW = 43;
    public final static int ARR_DEL15 = 44;
    public final static int UNIQUE_CARRIER = 6;
    public final static int AVG_TICKET_PRICE = 109;
}
