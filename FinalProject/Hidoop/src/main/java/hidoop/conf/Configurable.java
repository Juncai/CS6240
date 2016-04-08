package hidoop.conf;

/**
 * Created by jon on 4/7/16.
 */
public interface Configurable {
    void setConf(Configuration conf);

    Configuration getConf();
}
