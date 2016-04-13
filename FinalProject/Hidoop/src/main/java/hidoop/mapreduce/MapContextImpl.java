package hidoop.mapreduce;

import hidoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by jon on 4/9/16.
 */
public class MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    Configuration conf;
    RecordReader<KEYIN, VALUEIN> reader;
    String line;
    long inputCount;

    public MapContextImpl(Configuration conf,
                          RecordReader<KEYIN, VALUEIN> reader) {
        this.conf = conf;
        this.reader = reader;
        line = null;
        inputCount = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return reader.getCurrentValue();
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {

    }
}
