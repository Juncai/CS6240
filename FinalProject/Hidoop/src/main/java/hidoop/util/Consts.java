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
    public static final String COMMA = ",";
    public static final String DELIMITER = ", ";

    // data buffer file prefix
    public static final String BUFFER_FILE_PREFIX = "/dev/shm/buffer_";

    // regex pattern
    public static final String BUCKET_GROUP = "bucket";
    public static final String DIR_GROUP = "dir";
    public static final String S3_URL_PATTERN = "s3://(?<" + BUCKET_GROUP + ">\\w+)/(?<" + DIR_GROUP + ">\\w+)";

    // Communication headers
    public static final String RUNNING = "RUNNING";
    public static final String RUN_MAP = "MAP";
    public static final String MAP_DONE = "MAPPER_DONE";
    public static final String MAP_FAILED = "MAPPER_FAILED";
    public static final String RUN_PARTITION = "PARTITION";
    public static final String REDUCER_INPUT = "REDUCER_INPUT";
    public static final String PARTITION_DONE = "PARTITIONER_DONE";
    public static final String PARTITION_FAILED = "PARTITIONER_FAILED";
    public static final String REDUCER_INPUT_READY = "REDUCER_INPUT_READY";
    public static final String RUN_REDUCE = "REDUCE";
    public static final String REDUCE_DONE = "REDUCER_DONE";
    public static final String REDUCE_FAILED = "REDUCER_FAILED";
    public static final String SHUT_DOWN = "SHUT_DOWN";
    public static final String JOB_STATUS = "JOB_STATUS";
}
