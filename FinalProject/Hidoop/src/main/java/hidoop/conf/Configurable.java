package hidoop.conf;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public interface Configurable {
    void setConf(Configuration conf);
    Configuration getConf();
}
