package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.util.Consts;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private Configuration conf;
    private String line;
    public long inputCount;
    private InputStream is;
    private BufferedReader br;
    private FileSystem fs;
    private Path outputPath;
    private Partitioner p;
    private Map<Integer, List<String>> partitionBuffer;
    private Counter mapOutputCounter;


    public MapContextImpl(Configuration conf, Path inputPath,
                          Path outputPath, FileSystem fs,
                          Partitioner<KEYOUT, VALUEOUT> p) throws IOException {
        this.conf = conf;
        this.fs = fs;
        this.outputPath = outputPath;
        is = fs.open(inputPath);
        br = new BufferedReader(new InputStreamReader(is));
        line = null;
        inputCount = 0;
        this.p = p;
        initPartitionBuffer();
        mapOutputCounter = new Counter();
    }

    private void initPartitionBuffer() {
        partitionBuffer = new HashMap<Integer, List<String>>();
        for (int i = 0; i < conf.reducerNumber; i++) {
            partitionBuffer.put(i, new ArrayList<String>());
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return (line = br.readLine()) != null;
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        try {
            return (KEYIN) (conf.mapInputKeyClass).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        try {
            return (VALUEIN) (conf.mapInputValueClass).getDeclaredConstructor(String.class).newInstance(line);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
        int reduceInd = p.getPartition(key, value, conf.reducerNumber);
        partitionBuffer.get(reduceInd).add(key.toString() + Consts.KEY_VALUE_DELI + value.toString());
        mapOutputCounter.increment();
    }

    public long getCounterValue() {
        return mapOutputCounter.getValue();
    }

    @Override
    public Map<Integer, List<String>> getOutputBuffer() {
        return partitionBuffer;
    }

    private void createOutput() throws IOException {
        Path outPath;
        for (int i = 0; i < conf.reducerNumber; i++) {
            outPath = Path.appendDirFile(outputPath, Consts.MAP_OUTPUT_PREFIX + i);
            fs.createOutputFile(outPath, partitionBuffer.get(i), true);
        }
        partitionBuffer.clear();
        partitionBuffer = null;
    }

    public void close() {
        try {
            if (br != null) br.close();
            if (is != null) is.close();
            if (conf.isLocalMode) {
                createOutput();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
