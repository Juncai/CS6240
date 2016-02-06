package analysis

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

// Author: Jun Cai
object OTPAnalysis {
  def main(args: Array[String]) {
    println("Demo: startup")

    // Make a job
    val job = Job.getInstance()
    job.setJarByClass(OTPAnalysis.getClass)
    job.setJobName("Demo")

    // Set classes mapper, reducer, input, output.
    job.setMapperClass(classOf[OTPMapper])
    job.setReducerClass(classOf[OTPReducer])
    //job.setCombinerClass(classOf[DemoCombiner])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Set up number of mappers, reducers.
    job.setNumReduceTasks(2)

    /* FileInputFormat.addInputPath(job, new Path(args(0))) */
    FileInputFormat.addInputPath(job, new Path("""/home/jon/Downloads/part"""))
    /* FileOutputFormat.setOutputPath(job, new Path(args(1))) */
    FileOutputFormat.setOutputPath(job, new Path("output"))

    // Actually run the thing.
    job.waitForCompletion(true)
  }
}

// Author: Jun Cai
class OTPMapper extends Mapper[Object, Text, Text, Text] {
  type Context = Mapper[Object, Text, Text, Text]#Context

  var carrier = new Text()
  var dateAndPrice = new Text()
  var badRecords = 0

//  var spaces = "\\s+".r
//  var one = new Text("1")

  override def setup(ctx: Context) {
    badRecords = 0
  }

  override def cleanup(ctx: Context): Unit = {
//    ctx.write(new Text("INVALID"), new Text(badRecords.toString))
    ctx.write(new Text(OTPConsts.INVALID), new Text(badRecords.toString))
  }

  override def map(_k: Object, value: Text, ctx: Context) {
    var line = value.toString
    DataPreprocessor.processLine(line, carrier, dateAndPrice)
    if (carrier.toString.equals(OTPConsts.INVALID)) {
      badRecords += 1
    } else {
      ctx.write(carrier, dateAndPrice)
    }
  }
}

// vim: set ts=4 sw=4 et:
// Author: Jun Cai
class OTPReducer extends Reducer[Text, Text, Text, Text] {
  type Context = Reducer[Text, Text, Text, Text]#Context

  var reduces = 0
  var valsSeen = 0

  override def setup(ctx: Context): Unit = {
    reduces = 0
    println("DemoReducer.setup")
  }

  override def cleanup(ctx: Context): Unit = {
    println("DemoReducer.cleanup")
    println("Reduces: " + reduces)
    println("Vals seen: " + valsSeen)
  }

  override def reduce(key: Text, jvals: java.lang.Iterable[Text], ctx: Context): Unit = {

    var count = 0
    val vals: Iterable[Text] = jvals.asScala
    if (key.toString.equals(OTPConsts.INVALID)) {
      var sum = 0
      vals.foreach(v => {
        sum += v.toString.toInt
        ctx.write(key, new Text(sum.toString))
      })

    } else {
      var monthPrices:Map[String, Double] = Map()

    }
    vals.foreach(v => {
      count += v.toString().toInt
      valsSeen += 1
    })

    ctx.write(word, new Text(count .toString()))
    reduces += 1
  }
}
