package hidoop.mapreduce;

import com.google.common.io.Files;
import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.mapreduce.lib.map.WrappedMapper;
import hidoop.mapreduce.lib.reduce.WrappedReducer;
import hidoop.util.Consts;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 4/9/16.
 */
public class LocalClient implements Client {
    private Configuration conf;
    private List<Path> mapInputPathList;
    private List<Path> reduceInputDirList;
    private Consts.Stages status;
    private FileSystem fs;
    private int numMapperTasks;
    private Counter MapOutputCounter;

    public LocalClient(Configuration conf) {
        this.conf = conf;
        status = Consts.Stages.DEFINE;
        reduceInputDirList = new ArrayList<Path>();
        this.MapOutputCounter = new Counter();
    }

    @Override
    public void submitJob() throws IOException, InterruptedException {
//        public <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void submitJob() throws IOException, InterruptedException {
        // create file system
        fs = FileSystem.get(conf);

        // get input file list
        mapInputPathList = fs.getFileList(new Path(conf.inputPath));
        numMapperTasks = mapInputPathList.size();
        int numReducers = (numMapperTasks / 2) > 1 ? numMapperTasks / 2 : 1;
        conf.setNumReduceTasks(numReducers);
        status = Consts.Stages.BOOSTRAP;

        // map
        runMapTask();

        // TODO prepare reducer input
        transferData();

        // TODO reduce
        runReduceTask();
    }

    private void transferData() throws IOException {
        File from;
        File to;
        Path reduceInputDir;
        Path reduceInput;
        for (int i = 0; i < conf.reducerNumber; i++) {
            reduceInputDir = new Path(Consts.REDUCE_INPUT_DIR_PRE + i);
            reduceInputDirList.add(reduceInputDir);
            fs.createDir(reduceInputDir);
            for (int j = 0; j < numMapperTasks; j++) {
                from = new File(Consts.MAP_OUTPUT_DIR_PRE + j + "/" + Consts.MAP_OUTPUT_PREFIX + i);
                reduceInput = Path.appendDirFile(reduceInputDir, Consts.REDUCE_INPUT_PREFIX + j);
                to = new File(reduceInput.toString());
                //System.out.println(from.toString() + "&&" + to.toString());
                Files.move(from, to);
            }
        }
    }

    private <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void runReduceTask() {
        try {
            ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context;
            Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context reducerContext;
            Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer = (Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) conf.reducerClass.newInstance();
            Path outputPath = null;
            Path outputDir = new Path(conf.outputPath);
            FileSystem.createDir(outputDir);
            int reduceInd = 0;
            List<Path> inputPathList;


            for (Path inputDir : reduceInputDirList) {
                inputPathList = fs.getFileList(inputDir);
                outputPath = Path.appendDirFile(outputDir, Consts.REDUCE_OUTPUT_PREFIX +"0000" + reduceInd);
                // prepare context
                context = new ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, inputPathList, outputPath, fs, reduceInd++);
                reducerContext = new WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getReducerContext(context);

                // reduce
                reducer.run(reducerContext);
                reducerContext.close();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void runMapTask() {
        try {
            MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context;
            Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext;
            Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper = (Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) conf.mapperClass.newInstance();
            Partitioner partitioner = (Partitioner) conf.partitionerClass.newInstance();
            Path outputPath = null;
            int mapInd = 0;

            for (Path inputPath : mapInputPathList) {
                outputPath = new Path(Consts.MAP_OUTPUT_DIR_PRE + mapInd++);
                FileSystem.createDir(outputPath);
                // prepare context
                context = new MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, inputPath, outputPath, fs, partitioner);
                mapperContext = new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getMapContext(context);

                // map
                mapper.run(mapperContext);
                this.MapOutputCounter.join(new Counter(mapperContext.getCounterValue()));
                mapperContext.close();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Consts.Stages getStatus() throws IOException, InterruptedException {
        return status;
    }
    @Override
    public Counter getCounter(){
        return this.MapOutputCounter;
    }

}
