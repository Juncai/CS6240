package hidoop.mapreduce;

import java.io.IOException;
import java.util.List;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.mapreduce.lib.reduce.WrappedReducer;
import hidoop.util.Consts;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class ReduceTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	public void runReduceTask(Path inputDir, int reduceInd, Configuration conf, FileSystem fs,
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer) throws IOException, InterruptedException{
		List<Path> inputPathList = fs.getFileList(inputDir);
		Path outputDir = new Path(conf.outputPath);
		FileSystem.createDir(outputDir);
		Path outputPath = Path.appendDirFile(outputDir, Consts.REDUCE_OUTPUT_PREFIX + reduceInd);
		
		
        // prepare context
		ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context = new ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, inputPathList, outputPath, fs, reduceInd++);
		Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context reducerContext = new WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getReducerContext(context);

        // reduce
        reducer.run(reducerContext);
        reducerContext.close();
	}
}
