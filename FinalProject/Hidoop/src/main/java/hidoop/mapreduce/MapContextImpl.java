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
    BufferedReader inputBr;
    String line;
    long inputCount;

    public MapContextImpl(Configuration conf, BufferedReader inputBr) {
        this.conf = conf;
        this.inputBr = inputBr;
        line = null;
        inputCount = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return (line = inputBr.readLine()) == null;
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return ;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {

    }
}
