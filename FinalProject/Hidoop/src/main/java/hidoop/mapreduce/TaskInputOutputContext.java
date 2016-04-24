package hidoop.mapreduce;

import java.io.Closeable;
import java.io.IOException;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public interface TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Closeable {

    public boolean nextKeyValue() throws IOException, InterruptedException;

    public KEYIN getCurrentKey() throws IOException, InterruptedException;

    public VALUEIN getCurrentValue() throws IOException, InterruptedException;

    public void write(KEYOUT key, VALUEOUT value)
            throws IOException, InterruptedException;

    public void close();
}
