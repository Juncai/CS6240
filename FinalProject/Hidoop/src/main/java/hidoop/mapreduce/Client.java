package hidoop.mapreduce;

import hidoop.util.Consts;

import java.io.IOException;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public interface Client {
    public abstract void submitJob() throws IOException, InterruptedException;
    public abstract Consts.Stages getStatus() throws IOException, InterruptedException;
    public abstract Counter getCounter();
}
