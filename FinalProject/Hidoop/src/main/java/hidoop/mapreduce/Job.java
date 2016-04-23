package hidoop.mapreduce;

import hidoop.conf.Configuration;

import java.io.File;
import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
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
//        JobConf jobConf = new JobConf(conf);
        return new Job(conf);
    }

    public void setJarByClass(Class<?> cls) {
        // do nothing
//        ensureState(JobState.DEFINE);
//        conf.setJarByClass(cls);
    }

    public void setJobName(String name) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setJobName(name);
    }

    public void setMapperClass(Class<? extends Mapper> cls
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setMapperClass(cls);
    }

    public void setCombinerClass(Class<? extends Reducer> cls
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setCombinerClass(cls);
    }

    public void setReducerClass(Class<? extends Reducer> cls
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setReducerClass(cls);
    }

    public void setPartitionerClass(Class<? extends Partitioner> cls
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setPartitionerClass(cls);
    }

    public void setOutputKeyClass(Class<?> theClass
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setOutputKeyClass(theClass);
    }

    public void setOutputValueClass(Class<?> theClass
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setOutputValueClass(theClass);
    }

    public void setMapOutputKeyClass(Class<?> theClass
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
        conf.setMapOutputKeyClass(theClass);
    }

    public void setMapOutputValueClass(Class<?> theClass
    ) throws IllegalStateException {
//        ensureState(JobState.DEFINE);
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
        // TODO cleanup should be in the client
//        cleanUpTmp();
        return true;
    }


    public Counters getCounters() {
        return this.counters;
    }

    public Configuration getConfiguration() {
        return conf;
    }

//
//    public boolean isComplete() throws IOException {
////        ensureState(JobState.RUNNING);
//        updateStatus();
//        return status.isJobComplete();
//    }
//
//    /**
//     * Check if the job completed successfully.
//     *
//     * @return <code>true</code> if the job succeeded, else <code>false</code>.
//     * @throws IOException
//     */
//    public boolean isSuccessful() throws IOException {
////        ensureState(JobState.RUNNING);
//        updateStatus();
//        return status.getState() == JobStatus.State.SUCCEEDED;
//    }
//
//    synchronized void updateStatus() throws IOException {
//        try {
//            this.status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
//                @Override
//                public JobStatus run() throws IOException, InterruptedException {
//                    return cluster.getClient().getJobStatus(status.getJobID());
//                }
//            });
//        } catch (InterruptedException ie) {
//            throw new IOException(ie);
//        }
//        if (this.status == null) {
//            throw new IOException("Job status not available ");
//        }
//        this.statustime = System.currentTimeMillis();
//    }
//
//    public JobStatus getStatus() throws IOException, InterruptedException {
//        ensureState(JobState.RUNNING);
//        updateStatus();
//        return status;
//    }


}
