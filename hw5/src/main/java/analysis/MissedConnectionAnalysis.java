package analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.DataPreprocessor;
import utils.OTPConsts;

/**
 * Created by Jun Cai on 2/14/2016
 */

public class MissedConnectionAnalysis extends Configured implements Tool {

    public int run (String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(MissedConnectionAnalysis.class);
        job.setJobName("MissedConnectionAnalysis");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AnalysisMapper.class);
        job.setReducerClass(AnalysisReducer.class);

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
