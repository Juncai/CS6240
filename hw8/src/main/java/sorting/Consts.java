package sorting;

/**
 * Created by jon on 3/23/16.
 */
public class Consts {
    public static final String END_OF_LINE = "\r\n";
    public static final String END_OF_DATA = "EOD";
    public static final String NODE_READY = "READY";
    public enum Stage {
        SAMPLE, SELECT, SORT
    }
    public static final int SAMPLE_FREQUENCY = 500;
}
