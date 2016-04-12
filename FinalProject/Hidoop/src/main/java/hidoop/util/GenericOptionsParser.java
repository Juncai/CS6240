package hidoop.util;

import hidoop.conf.Configuration;

/**
 * Created by jon on 4/12/16.
 */
public class GenericOptionsParser {
    private String[] args;
    public GenericOptionsParser(Configuration conf, String[] args) {
        this.args = args;
    }

    public String[] getRemainingArgs() {
        return args;
    }
}
