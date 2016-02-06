package main

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

// Author: Nat Tuck
object Demo {
    def main(args: Array[String]) {
        println("Demo: startup")

        // Make a job
        val job = Job.getInstance()
        job.setJarByClass(Demo.getClass)
        job.setJobName("Demo")

        // Set classes mapper, reducer, input, output.
        job.setMapperClass(classOf[DemoMapper])
        job.setReducerClass(classOf[DemoReducer])
        //job.setCombinerClass(classOf[DemoCombiner])

        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])

        // Set up number of mappers, reducers.
        job.setNumReduceTasks(2)

        FileInputFormat.addInputPath(job,  new Path("alice.txt"))
        FileOutputFormat.setOutputPath(job, new Path("out"))

        // Actually run the thing.
        job.waitForCompletion(true)
    }
}

// Author: Nat Tuck
class DemoMapper extends Mapper[Object, Text, Text, Text] {
    type Context = Mapper[Object, Text, Text, Text]#Context

    var maps = 0
    var outputs = 0

    var spaces = "\\s+".r
    var one = new Text("1")

    override def setup(ctx: Context) {
        println("DemoMapper.setup")
    }

    override def cleanup(ctx: Context) {
        println("DemoMapper.cleanup")
        println(s"DemoMapper maps: $maps")
        println(s"DemoMapper outputs: $outputs")
    }
    
    override def map(_k: Object, line: Text, ctx: Context) {
        maps += 1;
        
        val words = spaces.split(line.toString)
        words.foreach(w => {
            ctx.write(new Text(w), one)
            outputs += 1
        })
    }
    
}

// vim: set ts=4 sw=4 et:
// Author: Jun Cai
class DemoReducer extends Reducer[Text, Text, Text, Text] {
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

    override def reduce(word: Text, jvals: java.lang.Iterable[Text], ctx: Context): Unit = {
      var count = 0
      val vals: Iterable[Text] = jvals.asScala
      vals.foreach(v => {
        count += v.toString().toInt
        valsSeen += 1
      })

      ctx.write(word, new Text(count .toString()))
      reduces += 1
    }
}