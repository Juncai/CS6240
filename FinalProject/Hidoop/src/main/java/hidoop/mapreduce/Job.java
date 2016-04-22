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

//    public void setNumReduceTasks(int numReduceTasks) {
//        conf.setNumReduceTasks(numReduceTasks);
//    }

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
        cluster.getClient().submitJob();
        this.counters.getGroup("123")
                .setCounter(cluster
                        .getClient()
                        .getCounter());
//        if (state == JobState.DEFINE) {
//            submit();
//        }
//        if (verbose) {
//            monitorAndPrintJob();
//        } else {
//            // get the completion poll interval from the client.
//            int completionPollIntervalMillis =
//                    Job.getCompletionPollInterval(cluster.getConf());
//            while (!isComplete()) {
//                try {
//                    Thread.sleep(completionPollIntervalMillis);
//                } catch (InterruptedException ie) {
//                }
//            }
//        }
//        return isSuccessful();

//        try {
//            System.out.println(conf.mapperClass.getCanonicalName());
//            System.out.println(conf.mapperClass.getName());
//            String mName = conf.mapperClass.getName();
//            Mapper m = (Mapper) Mapper.class.getClassLoader().loadClass(mName).newInstance();
////            Mapper m = (Mapper)conf.mapperClass.newInstance();
//            m.run(null);
//            Reducer r = (Reducer) conf.reducerClass.newInstance();
//            r.run(null);

        // load class from URL
//            JarFile jarFile = new JarFile(pathToJar);
//            Enumeration e = jarFile.entries();
//
//            URL[] urls = {new URL("jar:file:" + pathToJar + "!/")};
//            URLClassLoader cl = URLClassLoader.newInstance(urls);
//
//            while (e.hasMoreElements()) {
//                JarEntry je = (JarEntry) e.nextElement();
//                if (je.isDirectory() || !je.getName().endsWith(".class")) {
//                    continue;
//                }
//                // -6 because of .class
//                String className = je.getName().substring(0, je.getName().length() - 6);
//                className = className.replace('/', '.');
//                Class c = cl.loadClass(className);
//
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            return false;
//        }
        cleanUpTmp();
        return true;
    }

    private void cleanUpTmp() {
        File dir = new File(conf.inputPath);
        File[] directoryListing = dir.listFiles();

        int numberOfMappers = directoryListing.length;
        File map_out;
        for(int i = 0; i < numberOfMappers; i++) {
            map_out = new File("/tmp/map_out_" + i);
            delete(map_out);
        }

        int numberOfReducers = conf.reducerNumber;
        File reduce_in;
        for(int i = 0; i < numberOfReducers; i++) {
            reduce_in = new File("/tmp/reduce_in_" + i);
            delete(reduce_in);
        }
    }

    private void delete(File toDel) {
        if(toDel.isDirectory()){
            if(toDel.list().length == 0) {
                toDel.delete();
            } else {
                File[] nestedFiles = toDel.listFiles();

                for (File toDelNestedFile : nestedFiles) {
                    delete(toDelNestedFile);
                }

                if(toDel.list().length == 0){
                    toDel.delete();
                }
            }
        } else {
            toDel.delete();
        }
    }

    public Counters getCounters() {
        return this.counters;
    }

    //    public void submit()
//            throws IOException, InterruptedException, ClassNotFoundException {
//        ensureState(JobState.DEFINE);
//        setUseNewAPI();
//        connect();
//        final JobSubmitter submitter =
//                getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
//        status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
//            public JobStatus run() throws IOException, InterruptedException,
//                    ClassNotFoundException {
//                return submitter.submitJobInternal(Job.this, cluster);
//            }
//        });
//        state = JobState.RUNNING;
//        LOG.info("The url to track the job: " + getTrackingURL());
//    }
//
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
