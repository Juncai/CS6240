package slave;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.mapreduce.ReduceContext;
import hidoop.mapreduce.ReduceContextImpl;
import hidoop.mapreduce.Reducer;
import hidoop.mapreduce.lib.reduce.WrappedReducer;
import hidoop.util.Consts;

import java.util.List;

/**
 * Created by jon on 4/8/16.
 */
public class ReducerRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private Configuration conf;
    private int reducerInd;
    public Path outputPath;
    private List<Path> inputPathList;
    private FileSystem fs;
    private ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context;
    private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context reducerContext;
    private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;


    public ReducerRunner(Configuration conf, FileSystem fs, int reducerInd) {
        // TODO parse the configString
        this.conf = conf;
        this.fs = fs;
        this.reducerInd = reducerInd;
        String indStr = String.format("%05d", reducerInd);
        outputPath = new Path(Consts.REDUCE_OUTPUT_PREFIX + indStr);
        Path inputDir = new Path(Consts.REDUCE_INPUT_DIR_PRE + reducerInd);
        inputPathList = fs.getFileList(inputDir);
    }


    public boolean run() {
        try {
            reducer = (Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) conf.reducerClass.newInstance();
            context = new ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, inputPathList, outputPath, fs, reducerInd);
            reducerContext = new WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getReducerContext(context);

            // reduce
            reducer.run(reducerContext);
            reducerContext.close();
//            for (Path inputDir : reduceInputDirList) {
//                inputPathList = fs.getFileList(inputDir);
//                outputPath = Path.appendDirFile(outputDir, Consts.REDUCE_OUTPUT_PREFIX + indStr);
//                // prepare context
//
//            }

            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

}
