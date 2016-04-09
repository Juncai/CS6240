package hidoop.mapreduce;

import hidoop.util.Consts;

import java.io.IOException;

/**
 * Created by jon on 4/9/16.
 */
public interface Client {
    public abstract void submitJob() throws IOException, InterruptedException;
    public abstract Consts.Stages getStatus() throws IOException, InterruptedException;
}
