package hidoop.mapreduce;

import hidoop.conf.Configuration;

import java.io.File;
import java.io.IOException;

// Author: Jun Cai, Vikas Boddu
// Reference: github.com/apache/hadoop
public class Job {
    private Configuration conf;
    private Cluster cluster;
    private Counters counters;

    public Job(Configuration conf) throws IOException {
        // propagate existing user credentials to job
        this.cluster = null;
        this.conf = conf;
        this.counters = new Counters();
    }

    public Job(Configuration conf, String name) throws IOException {
        // propagate existing user credentials to job
        this.cluster = null;
        this.conf = conf;
        conf.jobName = name;
        this.counters = new Counters();
    }

    public static Job getInstance() throws IOException {
        // create with a null Cluster
        return getInstance(new Configuration());
    }

    public static Job getInstance(Configuration conf) throws IOException {
        // create with a null Cluster
        return new Job(conf);
    }

    public void setJarByClass(Class<?> cls) {
        // do nothing
    }

    public void setJobName(String name) throws IllegalStateException {
        conf.setJobName(name);
    }

    public void setMapperClass(Class<? extends Mapper> cls
    ) throws IllegalStateException {
        conf.setMapperClass(cls);
    }

    public void setCombinerClass(Class<? extends Reducer> cls
    ) throws IllegalStateException {
        conf.setCombinerClass(cls);
    }

    public void setReducerClass(Class<? extends Reducer> cls
    ) throws IllegalStateException {
        conf.setReducerClass(cls);
    }

    public void setPartitionerClass(Class<? extends Partitioner> cls
    ) throws IllegalStateException {
        conf.setPartitionerClass(cls);
    }

    public void setOutputKeyClass(Class<?> theClass
    ) throws IllegalStateException {
        conf.setOutputKeyClass(theClass);
    }

    public void setOutputValueClass(Class<?> theClass
    ) throws IllegalStateException {
        conf.setOutputValueClass(theClass);
    }

    public void setMapOutputKeyClass(Class<?> theClass
    ) throws IllegalStateException {
        conf.setMapOutputKeyClass(theClass);
    }

    public void setMapOutputValueClass(Class<?> theClass
    ) throws IllegalStateException {
        conf.setMapOutputValueClass(theClass);
    }


    public void setNumReduceTasks(int num) {
        conf.setNumReduceTasks(num);
    }

    public boolean waitForCompletion(boolean verbose
    ) throws IOException, InterruptedException,
            ClassNotFoundException {
        // TODO create cluster with the configuration
        cluster = new Cluster(conf);
        
        long start = System.currentTimeMillis();
        cluster.getClient().submitJob();
        long finish = System.currentTimeMillis();
        
        // printout task lasting time
        System.out.println("the job lasting time is: " + ((finish-start)/1000) + "s");
        
        counters.getGroup("123")
                .setCounter(cluster
                        .getClient()
                        .getCounter());
        return true;
    }


    public Counters getCounters() {
        return this.counters;
    }

    public Configuration getConfiguration() {
        return conf;
    }
}
