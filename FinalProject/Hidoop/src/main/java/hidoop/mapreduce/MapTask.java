package hidoop.mapreduce;

import java.io.IOException;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.mapreduce.lib.map.WrappedMapper;
import hidoop.util.Consts;

// Author: Xi Wang
// Reference: github.com/apache/hadoop
public class MapTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public void runMapTask(Path inputPath, int mapInd, Configuration conf, FileSystem fs,
                           Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper) throws InstantiationException, IllegalAccessException, IOException, InterruptedException {

        Partitioner partitioner = (Partitioner) conf.partitionerClass.newInstance();


        Path outputPath = new Path(Consts.MAP_OUTPUT_DIR_PRE + mapInd++);
        FileSystem.createDir(outputPath);
        // prepare context
        MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context = new MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, inputPath, outputPath, fs, partitioner);
        Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext = new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getMapContext(context);

        // map
        mapper.run(mapperContext);
        mapperContext.close();
    }

}
