package hidoop.mapreduce;

import java.io.IOException;

/**
 * Created by jon on 4/9/16.
 */
public class ReduceContextImpl implements ReduceContext {
    @Override
    public boolean nextKey() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public Iterable getValues() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {

    }
}
