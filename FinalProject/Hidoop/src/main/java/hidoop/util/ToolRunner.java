package hidoop.util;

import hidoop.conf.Configuration;

/**
 * Created by jon on 4/6/16.
 */
public class ToolRunner {
    public static int run(Configuration conf, Tool tool, String[] args)
            throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
//        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        //set the configuration back, so that Tool can configure itself
        tool.setConf(conf);

        //get the args w/o generic hadoop args
//        String[] toolArgs = parser.getRemainingArgs();
//        return tool.run(toolArgs);
        return tool.run(args);
    }

    /**
     * Runs the <code>Tool</code> with its <code>Configuration</code>.
     * <p/>
     * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
     *
     * @param tool <code>Tool</code> to run.
     * @param args command-line arguments to the tool.
     * @return exit code of the {@link Tool#run(String[])} method.
     */
    public static int run(Tool tool, String[] args)
            throws Exception {
        return run(tool.getConf(), tool, args);
    }

}
