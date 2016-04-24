package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.fs.FileSystem;
import hidoop.fs.Path;
import hidoop.util.Consts;
import hidoop.util.InputUtils;
//import jdk.internal.util.xml.impl.Input;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private Configuration conf;
    private String line;
    public long inputCount;
    private InputStream is;
    private BufferedReader br;
    private OutputStream os;
    private BufferedWriter bw;
    private FileSystem fs;
    private Path outputPath;
    private List<Path> inputPathList;
    private Path keyValueFile;
    private int reducerInd;
    private String[] nextKV;

    public ReduceContextImpl(Configuration conf, List<Path> inputPathList,
                             Path outputPath, FileSystem fs, int reducerInd) throws IOException {
        this.conf = conf;
        this.fs = fs;
        this.outputPath = outputPath;
        this.inputPathList = inputPathList;
        this.reducerInd = reducerInd;
        line = null;
        inputCount = 0;
        sortAndBufferedInput();
        prepareOutputWriter();
    }

    private void prepareOutputWriter() throws IOException {
        os = fs.create(outputPath);
        bw = new BufferedWriter(new OutputStreamWriter(os));
    }

    public void sortAndBufferedInput() throws IOException {
        Map<String, List<String>> keyValueMap = new HashMap<String, List<String>>();
        String key;
        String[] kv;
        InputStream iss;
        BufferedReader brr;
        for (Path p : inputPathList) {
            iss = fs.open(p);
            brr = new BufferedReader(new InputStreamReader(iss));
            while ((line = brr.readLine()) != null) {
                kv = InputUtils.extractKey(line);
                key = kv[0];
                if (!keyValueMap.containsKey(key)) {
                    keyValueMap.put(key, new ArrayList<String>());
                }
                keyValueMap.get(key).add(line);
            }
            brr.close();
            iss.close();
            FileSystem.removeFile(p);
        }
        List<String> sortedKeys = new ArrayList<String>(keyValueMap.keySet());
        Collections.sort(sortedKeys, new Comparator<String>() {
            public int compare(String o1, String o2) {
                try {
                    int value1 = Integer.valueOf(o1);
                    int value2 = Integer.valueOf(o2);
                    return value1 > value2 ? 1 : (value1 == value2 ? 0 : -1);
                } catch (NumberFormatException ex) {
                    return o1.compareTo(o2);
                }

            }
        });
        /*TreeSet<String> sortedKeys = new TreeSet<String>(new Comparator<String>()
        {
            public int compare(String o1, String o2) {
                return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
            }
        });*/

        Path dir = new Path(Consts.REDUCE_INPUT_DIR_PRE + reducerInd);
        keyValueFile = Path.appendDirFile(dir, "kv_buffered");
        OutputStream oss = fs.create(keyValueFile);
        BufferedWriter bww = new BufferedWriter(new OutputStreamWriter(oss));
        for (String k : sortedKeys) {
            for (String l : keyValueMap.get(k)) {
                bww.write(l + Consts.END_OF_LINE);
            }
        }
        bww.flush();
        bww.close();
        oss.close();
        // try to release memory
        keyValueMap.clear();
        keyValueMap = null;

        // prepare input stream
        is = fs.open(keyValueFile);
        br = new BufferedReader(new InputStreamReader(is));
        if ((line = br.readLine()) != null) {
            nextKV = InputUtils.extractKey(line);
        }
    }

    @Override
    public boolean nextKey() throws IOException, InterruptedException {
        return nextKV != null;
    }

    @Override
    public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
        try {
            List<VALUEIN> values = new ArrayList<VALUEIN>();

            // add the first value
            VALUEIN v = (VALUEIN) (conf.mapOutputValueClass).getDeclaredConstructor(String.class)
                    .newInstance(nextKV[1]);
            values.add(v);

            String[] kv;
            while (true) {
                line = br.readLine();
                if (line == null || line.trim().length() == 0) {
                    nextKV = null;
                    break;
                }
                kv = InputUtils.extractKey(line);
                if (!kv[0].equals(nextKV[0])) {
                    nextKV = kv;
                    break;
                }
                v = (VALUEIN) (conf.mapOutputValueClass).getDeclaredConstructor(String.class)
                        .newInstance(kv[1]);
                values.add(v);
            }
            return values;
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
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // ignore
        return false;
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        try {
            String key = nextKV[0];
            return (KEYIN) (conf.mapOutputKeyClass).getDeclaredConstructor(String.class).newInstance(key);
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
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        // ignore
        return null;
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
        bw.write(key.toString() + Consts.KEY_VALUE_DELI + value.toString() + Consts.END_OF_LINE);
    }

    @Override
    public void close() {
        try {
            if (br != null) br.close();
            if (is != null) is.close();
            if (bw != null) {
                bw.flush();
                bw.close();
            }
            if (os != null) os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
