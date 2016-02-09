package analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.OTPConsts;

/**
 * Created by Jun Cai on 1/24/16.
 */

public class LinearRegressionFit extends Configured implements Tool {

    public int run (String[] args) throws Exception {

        Job job = new Job();
        job.setJarByClass(LinearRegressionFit.class);
        job.setJobName("LinearRegressionFit");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FittingMapper.class);
        job.setReducerClass(FittingReducer.class);
			

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set separator in the output to be ","
        Configuration conf = job.getConfiguration();
        // Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinearRegressionFit(), args));
	}
}
