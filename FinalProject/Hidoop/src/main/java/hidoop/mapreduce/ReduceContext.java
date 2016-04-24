package hidoop.mapreduce;

import java.io.IOException;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public interface ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    public boolean nextKey() throws IOException, InterruptedException;

    public Iterable<VALUEIN> getValues() throws IOException, InterruptedException;
}
