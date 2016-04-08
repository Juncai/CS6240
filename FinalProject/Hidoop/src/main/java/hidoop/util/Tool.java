package hidoop.util;

import hidoop.conf.Configurable;

/**
 * Created by jon on 4/7/16.
 */
public interface Tool extends Configurable {
    int run(String[] args) throws Exception;
}
