package analysis

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
//import java.util._
//import java.lang._
import scala.collection.JavaConverters._

import scala.collection.mutable

// Author: Jun Cai
object OTPAnalysisPD {
//  private var conf = new Configuration()
//  private var maprfsCoreSitePath = new Path("core-site.xml")
//  private var maprfsSitePath = new Path("maprfs-site.xml")
//  def main(args: Array[String]) {
//    conf.addResource(maprfsCoreSitePath)
//    conf.addResource(maprfsSitePath)
//    // Make a job
//    val job = Job.getInstance()
//    job.setJarByClass(OTPAnalysis.getClass)
//    job.setJobName("Demo")
//
//    // Set classes mapper, reducer, input, output.
//    job.setMapperClass(classOf[OTPMapper])
//    job.setReducerClass(classOf[OTPReducer])
//    //job.setCombinerClass(classOf[DemoCombiner])
//
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[Text])
//
//    // Set up number of mappers, reducers.
//    job.setNumReduceTasks(2)
//
//    /* FileInputFormat.addInputPath(job, new Path(args(0))) */
//    FileInputFormat.addInputPath(job, new Path("""/home/jon/Downloads/all"""))
//    /* FileOutputFormat.setOutputPath(job, new Path(args(1))) */
//    FileOutputFormat.setOutputPath(job, new Path("output"))
//
//    // Actually run the thing.
//    job.waitForCompletion(true)
//  }
}

