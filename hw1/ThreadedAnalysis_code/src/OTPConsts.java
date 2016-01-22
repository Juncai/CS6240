/**
 * Created by jonca on 1/12/2016.
 *
 * #### My result with input file 323.csv.gz ######
     4082
     435940
     WN 57.79
     HP 65.39
     AS 74.96
     PI 189.67
     CO 203.56
     EA 269.25
     PA (1) 283.09
     US 287.81
     TW 295.68
     AA 304.96
     DL 346.31
     UA 541.65
     NW 53856.85
 * ################### Note about header handling ###################
 * I didn't include the header line as a record, so my K would be one
 * smaller than the reference result (4082 instead of 4083).
 *
 * ##################################################################
 *
 * Input argument: path to gzip file
 * Output:
 *      K (number of corrupted lines)
 *      F (number of sane flights)
 *      C p (where C is the carrier code, p is the mean price of tickets)
 */

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class OTPConsts {

    // constants for indices
    private final static int FL_DATE = 5;
    private final static int CRS_ARR_TIME = 40;
    private final static int CRS_DEP_TIME = 29;
    private final static int CRS_ELAPSED_TIME = 50;
    private final static int[] NOTZERO = {29, 40};

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
    private final static int[] LARGERTHANZERO = {11, 12, 13, 17, 19, 20, 21, 22, 26, 28};

    // ORIGIN 14
    // ORIGIN_CITY_NAME 15
    // ORIGIN_STATE_ABR 16
    // ORIGIN_STATE_NM 18
    // DEST 23
    // DEST_CITY_NAME 24
    // DEST_STATE_ABR 25
    // DEST_STATE_NM 27
    private final static int[] NOTEMPTY = {14, 15, 16, 18, 23, 24, 25, 27};

    private final static int CANCELLED = 47;
    private final static int DEP_TIME = 30;
    private final static int ARR_TIME = 41;
    private final static int ACTUAL_ELAPSED_TIME = 51;
    private final static int ARR_DELAY = 42;
    private final static int ARR_DELAY_NEW = 43;
    private final static int ARR_DEL15 = 44;
    private final static int UNIQUE_CARRIER = 6;
    private final static int AVG_TICKET_PRICE = 109;
}