package hidoop.mapreduce;

import java.io.IOException;

/**
 * Created by jon on 4/9/16.
 */
public class MapContextImpl implements MapContext {
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
