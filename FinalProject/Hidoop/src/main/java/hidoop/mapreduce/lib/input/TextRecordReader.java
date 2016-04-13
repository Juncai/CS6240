package hidoop.mapreduce.lib.input;

import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.io.LongWritable;
import hidoop.io.Text;
import hidoop.mapreduce.RecordReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by jon on 4/12/16.
 */
public class TextRecordReader extends RecordReader <Object, Text> {
    private InputStream is;

    @Override
    public void init(List<Path> inputs, FileSystem fs) throws IOException, InterruptedException {
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
    public Text getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
