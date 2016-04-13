package hidoop.mapreduce;

import hidoop.fs.FileSystem;
import hidoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by jon on 4/12/16.
 */
public abstract class RecordReader<KEYIN, VALUEIN> implements Cloneable {
    public abstract void init(List<Path> inputs, FileSystem fs) throws IOException, InterruptedException;
    public abstract boolean nextKeyValue() throws IOException, InterruptedException;

    public abstract KEYIN getCurrentKey() throws IOException, InterruptedException;

    public abstract VALUEIN getCurrentValue() throws IOException, InterruptedException;

    public abstract void close() throws IOException;
}