package hidoop.util;

/**
 * Created by jon on 4/7/16.
 */
public class Consts {
    public enum Stages {
        DEFINE, BOOSTRAP, MAP, PARTITION, REDUCE, DONE
    }


    public static final String MAP_OUTPUT_DIR = "map_out";
    public static final String REDUCE_INPUT_DIR = "reduce_in";
    public static final String REDUCE_OUTPUT_DIR = "reduce_out";

        public static final String END_OF_LINE = "\r\n";
    public static final String END_OF_LINE_L = "\n";
    public static final String END_OF_DATA = "EOD";
    public static final String END_OF_DATA_EOL = END_OF_DATA + END_OF_LINE;
    public static final String HEADER_START = "Wban Number,";
    public static final String NODE_READY = "READY";
    public static final String NODE_READY_EOL = NODE_READY + END_OF_LINE;
    public static final String COMMA = ",";
    public static final String DELIMITER = ", ";

    // Communication headers
    public static final String MASTER_HEADER = "MASTER";
    public static final String SAMPLE_HEADER = "SAMPLE";
    public static final String DATA_HEADER = "DATA";
    public static final String READY_HEADER = "READY";

    // master request
    public static final String STATUS_REQ = "STATUS";
    public static final String SHUTDOWN_REQ = "SHUTDOWN";
    public static final String FINISHED = "FININSHED";
    public static final String FINISHED_EOL = FINISHED + END_OF_LINE;
    public static final String WORKING = "WORKING";
    public static final String WORKING_EOL = WORKING + END_OF_LINE;

    // data buffer file prefix
    public static final String BUFFER_FILE_PREFIX = "/dev/shm/buffer_";

    // regex pattern
    public static final String BUCKET_GROUP = "bucket";
    public static final String DIR_GROUP = "dir";
    public static final String S3_URL_PATTERN = "s3://(?<" + BUCKET_GROUP + ">\\w+)/(?<" + DIR_GROUP + ">\\w+)";

}
