package hidoop.conf;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Configured implements Configurable {
    private Configuration conf;

    public Configured() {
        this(null);
    }

    public Configured(Configuration conf) {
        setConf(conf);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
