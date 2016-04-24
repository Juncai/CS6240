package hidoop.util;

import hidoop.conf.Configurable;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public interface Tool extends Configurable {
    int run(String[] args) throws Exception;
}
