package job.assignment2;

import hidoop.conf.Configuration;
import hidoop.conf.Configured;
import hidoop.fs.Path;
import hidoop.io.DoubleWritable;
import hidoop.io.Text;
import hidoop.mapreduce.FileInputFormat;
import hidoop.mapreduce.FileOutputFormat;
import hidoop.mapreduce.Job;
import hidoop.util.Tool;
import hidoop.util.ToolRunner;


/**
 * Created by phoenix on 1/24/16.
 */

public class ClusterAnalysis extends Configured implements Tool {

    public int run (String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(ClusterAnalysis.class);
        job.setJobName("ClusterAnalysis");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(OTPMapper.class);
        job.setReducerClass(OTPReducer.class);
        job.setNumReduceTasks(4);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set separator in the output to be ","
        Configuration conf = job.getConfiguration();
        // Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ClusterAnalysis(), args));
	}
}