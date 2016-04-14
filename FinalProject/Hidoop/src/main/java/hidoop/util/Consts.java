package hidoop.util;

/**
 * Created by jon on 4/7/16.
 */
public class Consts {
    public enum Stages {
        DEFINE, BOOSTRAP, MAP, PARTITION, REDUCE, DONE
    }

    public enum TaskStatus {
        DEFINE, READY, RUNNING, DONE, FAILED
    }

    public enum NodeStatus {
        OFFLINE, IDLE, BUSY
    }


    public static final String END_OF_LINE = "\r\n";
    public static final String END_OF_LINE_L = "\n";
    public static final String END_OF_DATA = "EOD";
    public static final String END_OF_DATA_EOL = END_OF_DATA + END_OF_LINE;
    public static final String COMMA = ",";
    public static final String KEY_VALUE_DELI = "\t";

    // data buffer file prefix
    public static final String BUFFER_FILE_PREFIX = "/dev/shm/buffer_";

    // regex pattern
    public static final String BUCKET_GROUP = "bucket";
    public static final String KEY_GROUP = "key";
    public static final String S3_URL_PATTERN = "s3://(?<" + BUCKET_GROUP +
            ">\\w+)/(?<" + KEY_GROUP + ">\\.+)";

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

    // config file path
    public static final String CONFIG_DIR = "./config";
    public static final String IP_LIST_PATH = CONFIG_DIR + "/ips";
    public static final String CONFIG_PATH = CONFIG_DIR + "/hidoop.conf";

    // config consts
    public static final String LOCAL_MODE = "LOCAL";
    public static final String EC2_MODE = "EC2";

    // intput/output path related
    public enum FileSystemTypes {
        LOCAL, S3
    }
    public static final String S3_URL_PREFIX = "s3://";
    public static final String TXT_EXT = ".txt";
    public static final String TAR_GZ_EXT = ".tar.gz";
    public static final String MAP_OUTPUT_DIR_PRE = "/tmp/map_out_";
    public static final String MAP_OUTPUT_PREFIX = "reducer_";
    public static final String REDUCE_INPUT_DIR_PRE = "/tmp/reduce_in_";
    public static final String REDUCE_INPUT_PREFIX = "mapper_";
    public static final String REDUCE_OUTPUT_PREFIX = "part-r-";

}
