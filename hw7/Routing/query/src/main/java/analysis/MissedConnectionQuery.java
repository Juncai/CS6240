package analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// Authors: Jun Cai and Vikas Boddu
public class MissedConnectionQuery extends Configured implements Tool {

    public int run (String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(MissedConnectionQuery.class);
        job.setJobName("MissedConnectionQuery");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(QueryMapper.class);
        job.setReducerClass(QueryReducer.class);

	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MissedConnectionQuery(), args));
	}
}
