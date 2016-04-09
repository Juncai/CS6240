package hidoop.mapreduce;

import hidoop.util.Consts;

import java.io.IOException;

/**
 * Created by jon on 4/9/16.
 */
public class EC2Client implements Client {
    @Override
    public void submitJob() throws IOException, InterruptedException {

    }

    @Override
    public Consts.Stages getStatus() throws IOException, InterruptedException {
        return null;
    }
}
