package analysis;

import hidoop.conf.Configuration;
import hidoop.conf.Configured;
import hidoop.fs.Path;
import hidoop.io.IntWritable;
import hidoop.io.Text;
import hidoop.mapreduce.Job;
import hidoop.mapreduce.FileInputFormat;
import hidoop.mapreduce.FileOutputFormat;
import hidoop.util.Tool;
import hidoop.util.ToolRunner;


// Authors: Jun Cai and Vikas Boddu
public class MissedConnectionAnalysis extends Configured implements Tool {

    public int run (String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(MissedConnectionAnalysis.class);
        job.setJobName("MissedConnectionAnalysis");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AnalysisMapper.class);
        job.setReducerClass(AnalysisReducer.class);
        job.setPartitionerClass(AnalysisPartitioner.class); // set custom partitioner
//        job.setNumReduceTasks(2); // for test

		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MissedConnectionAnalysis(), args));
	}
}
