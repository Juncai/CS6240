package hidoop.util;

import hidoop.conf.Configuration;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class ToolRunner {
    public static int run(Configuration conf, Tool tool, String[] args)
            throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        //set the configuration back, so that Tool can configure itself
        tool.setConf(conf);

        //get the args w/o generic hadoop args
        return tool.run(args);
    }

    public static int run(Tool tool, String[] args)
            throws Exception {
        return run(tool.getConf(), tool, args);
    }

}
