package hidoop.mapreduce;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.util.Consts;
import hidoop.util.InputUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by jon on 4/9/16.
 */
public class LocalClient implements Client {
    private Configuration conf;
    private List<Path> inputPathList;
    private Consts.Stages status;

    public LocalClient(Configuration conf) {
        this.conf = conf;
        status = Consts.Stages.DEFINE;
    }

    @Override
    public <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void submitJob() throws IOException, InterruptedException {
        // create file system
        FileSystem fs = FileSystem.get(conf);

        // get input file list
        inputPathList = fs.getInputList();

        status = Consts.Stages.BOOSTRAP;


        // TODO map
//        MapContext<conf.mapInputKeyClass, conf.mapInputValueClass> context = new MapContextImpl(conf);
        MapContext context;
        Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;
        String dataLine;

        // TODO load input from s3, parse and sample the data
        BufferedReader inputBr;
        try {
            for (Path inputPath : inputPathList) {
                InputStream is = fs.open(inputPath);
                inputBr = new BufferedReader(new InputStreamReader(is));
                // TODO prepare context with reader
                context = new MapContextImpl(conf, inputBr);

                inputBr.close();
                is.close();

                // TODO map
                mapper = (Mapper) conf.mapperClass.newInstance();
                mapper.run((Mapper.Context) context);

                // TODO handle output in the context with PARTITIONER!
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }


        // TODO send reduce input to reducer

    }

    @Override
    public Consts.Stages getStatus() throws IOException, InterruptedException {
        return status;
    }
}
