package hidoop.util;

import hidoop.conf.Configuration;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class GenericOptionsParser {
    private String[] args;
    public GenericOptionsParser(Configuration conf, String[] args) {
        this.args = args;
    }

    public String[] getRemainingArgs() {
        return args;
    }
}
