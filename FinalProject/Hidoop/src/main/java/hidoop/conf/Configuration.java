package hidoop.conf;

import hidoop.fs.Path;
import hidoop.mapreduce.Mapper;
import hidoop.mapreduce.Partitioner;
import hidoop.mapreduce.Reducer;

/**
 * Created by jon on 4/6/16.
 */
public class Configuration {
    public boolean isLocalMode;
    public String jobName;
    public Class mapperClass;
    public Class reducerClass;
    public Class partitionerClass;
    public Class outputKeyClass;
    public Class outputValueClass;
    public Class mapOutputKeyClass;
    public Class mapOutputValueClass;
    public Path inputPath;
    public Path outputPath;


    public Configuration() {
        isLocalMode = true;
        // TODO load local config file
        // mode: local/S3
        // IPs
        // AWS credentials
    }

    public void setJobName(String jname) {
        this.jobName = jname;
    }

    public void setMapperClass(Class<? extends Mapper> cls) {
        this.mapperClass = cls;
    }

    public void setReducerClass(Class<? extends Reducer> cls) {
        this.reducerClass = cls;
    }

    public void setPartitionerClass(Class<? extends Partitioner> cls) {
        this.partitionerClass = cls;
    }

    public void setOutputKeyClass(Class<?> cls) {
        this.outputKeyClass = cls;
    }

    public void setOutputValueClass(Class<?> cls) {
        this.outputValueClass = cls;
    }

    public void setMapOutputKeyClass(Class<?> cls) {
        this.mapOutputKeyClass = cls;
    }

    public void setMapOutputValueClass(Class<?> cls) {
        this.mapOutputValueClass = cls;
    }

    public void setInputPath(Path p) {
        this.inputPath = p;
    }
    public void setOutputPath(Path p) {
        this.outputPath = p;
    }

    public void set(String name, String value) {
        // do nothing;
//        set(name, value, null);
    }

}
