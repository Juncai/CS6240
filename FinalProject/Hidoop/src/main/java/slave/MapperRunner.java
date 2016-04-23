package slave;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.mapreduce.*;
import hidoop.mapreduce.lib.map.WrappedMapper;
import hidoop.util.Consts;

import java.util.List;
import java.util.Map;

/**
 * Created by jon on 4/8/16.
 */
public class MapperRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private Configuration conf;
    private int mapperInd;
    private Path inputPath;
    private Path outputDir;
    private FileSystem fs;
    public Map<Integer, List<String>> outputBuffer;
    private MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context;
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext;
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;
    public long counter;


    public MapperRunner(Configuration conf, FileSystem fs, int mapperInd, Path inputPath) {
        counter = 0;
        this.conf = conf;
        this.fs = fs;
        this.mapperInd = mapperInd;
        this.inputPath = inputPath;
        outputDir = new Path(Consts.MAP_OUTPUT_DIR_PRE + mapperInd);
    }

    public boolean run() {
        try {
            mapper = (Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) this.conf.mapperClass.newInstance();

            Partitioner partitioner = (Partitioner) conf.partitionerClass.newInstance();
            FileSystem.createDir(outputDir);

            // prepare context
            context = new MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, inputPath, outputDir, fs, partitioner);
            mapperContext = new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getMapContext(context);

            // map
            mapper.run(mapperContext);
            counter = mapperContext.getCounterValue();
            outputBuffer = mapperContext.getOutputBuffer();
            mapperContext.close();
            mapperContext = null;

            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }
}
