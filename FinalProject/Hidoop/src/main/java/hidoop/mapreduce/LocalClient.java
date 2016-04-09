package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.util.Consts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 4/9/16.
 */
public class LocalClient implements Client {
    private Configuration conf;
    private List<String> inputPathList;
    private Consts.Stages status;

    public LocalClient(Configuration conf) {
        this.conf = conf;
        status = Consts.Stages.DEFINE;
    }

    @Override
    public void submitJob() throws IOException, InterruptedException {
        // TODO prepare input file list
        inputPathList = new ArrayList<String>();
        status = Consts.Stages.BOOSTRAP;


        // TODO map
        MapContext context = new MapContextImpl();
        Mapper mapper;
        try {
            for (String inputPath : inputPathList) {
                // TODO prepare context

                // TODO map
                mapper = (Mapper) conf.mapperClass.newInstance();
                mapper.run((Mapper.Context) context);

                // TODO handle output in the context
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public Consts.Stages getStatus() throws IOException, InterruptedException {
        return status;
    }
}
