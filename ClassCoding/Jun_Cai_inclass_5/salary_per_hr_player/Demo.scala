package main

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

import scala.collection.JavaConverters._
//Starting from the assignment download below, which includes the Baseball data,
// write a map-reduce job which will calculate, for each (player, year),
// how much the player was paid per home run.

//Bonus task: Further modify your program to calculate average cost per home run for each (team, year).
// Author: Nat Tuck
object Demo {
  def main(args: Array[String]) {
    println("Demo: startup")

    // Configure log4j to actually log.
    BasicConfigurator.configure();

    // Make a job
    val job = Job.getInstance()
    job.setJarByClass(Demo.getClass)
    job.setJobName("Demo")

    // Set classes mapper, reducer, input, output.
    job.setReducerClass(classOf[DemoReducer])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])

    // Set up number of mappers, reducers.
//    job.setNumReduceTasks(2)

    MultipleInputs.addInputPath(job, new Path("baseball/Batting.csv"),
      classOf[TextInputFormat], classOf[HRMapper])
    MultipleInputs.addInputPath(job, new Path("baseball/Salaries.csv"),
      classOf[TextInputFormat], classOf[SalaryMapper])

    FileOutputFormat.setOutputPath(job, new Path("out"))

    // Actually run the thing.
    job.waitForCompletion(true)
  }
}

// Author: Jun Cai
class HRMapper extends Mapper[Object, Text, Text, Text] {
  type Context = Mapper[Object, Text, Text, Text]#Context

  val pInt = "[0-9]+".r

  override def map(_k: Object, vv: Text, ctx: Context) {
    val cols = vv.toString.split(",")
    if (cols(0) == "playerID" || cols.length < 12) {
      return
    }

    val playerID = cols(0).trim
    val year = cols(1).trim
    val hr = cols(11).trim
    var isInt = false

    if (hr != null && hr != "") {
      hr match {
        case pInt(_*) => isInt = true
      }
      if (isInt && hr.toInt > 0) {
        ctx.write(new Text(playerID + " " + year), new Text("HR " + hr))
      }
    }
  }
}

// Author: Jun Cai
class SalaryMapper extends Mapper[Object, Text, Text, Text] {
  type Context = Mapper[Object, Text, Text, Text]#Context

  val pInt = "[0-9]+".r

  override def map(_k: Object, vv: Text, ctx: Context) {
    val cols = vv.toString.split(",")
    if (cols(0) == "yearID" || cols.length < 5) {
      return
    }

    val year = cols(0).trim
    val playerID = cols(3).trim
    val salary = cols(4).trim
    if (salary != null && salary != "") {
      salary match {
        case pInt(_*) => ctx.write(new Text(playerID + " " + year), new Text("S " + salary))
      }
    }
  }
}

// Author: Jun Cai
class DemoReducer extends Reducer[Text, Text, Text, DoubleWritable] {
  type Context = Reducer[Text, Text, Text, DoubleWritable]#Context

  override def reduce(playerYear: Text, jvals: java.lang.Iterable[Text], ctx: Context): Unit = {
    var hr = 0
    var salary = 0
    var t = ""
    var v = ""
    val vals: Iterable[Text] = jvals.asScala
    vals.foreach(vv => {
      t = vv.toString().split(" ")(0)
      v = vv.toString().split(" ")(1)
      if (t.equals("HR")) {
        hr += v.toInt
      } else if (t.equals("S")) {
        salary += v.toInt
      }
    })
    if (hr > 0) {
      ctx.write(playerYear, new DoubleWritable(1.0 * salary / hr))
    }
  }
}

// vim: set ts=4 sw=4 et:
