package analysis

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer

//import java.util._
//import java.lang._
import scala.collection.JavaConverters._
//import analysis.DataProcess

import scala.collection.mutable

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
class OTPReducer extends Reducer[Text, Text, Text, DoubleWritable] {
  type Context = Reducer[Text, Text, Text, DoubleWritable]#Context

  override def reduce(key: Text, jvals: java.lang.Iterable[Text], ctx: Context): Unit = {

    println("See: " + key.toString)
    val values: Iterable[Text] = jvals.asScala

    if (key.toString.equals(OTPConsts.INVALID)) {
      var sum = 0
      values.foreach(v => {
        sum += v.toString.toInt
      })
      ctx.write(key, new DoubleWritable(sum))

    } else {
      var monthPrices:mutable.Map[String, ListBuffer[Double]] = mutable.Map()
      var month = ""
//      var price:java.util.List[Double] = java.util.ArrayList[Double]()
      var price:Double = 0
//      var prices:java.util.List[java.lang.Double] = new java.util.ArrayList[java.lang.Double]()
      var prices:ListBuffer[Double] = ListBuffer()
      var carrierMonth = new Text()
      var mean = new DoubleWritable()
      if (isActiveCarrier(values)) {
        var totalFl = 0
        values.foreach(v => {
          totalFl += 1
          month = getMonth(v)
          price = getPrice(v)
          if (monthPrices contains month) {
            monthPrices(month) += price
          } else {
            prices = ListBuffer()
            prices += price
            monthPrices(month) = prices
          }
        })

        monthPrices.keySet.foreach(mk => {
          carrierMonth.set(mk + "," + key.toString() + "," + totalFl)
          mean.set(getMean(monthPrices(mk).toList))
          ctx.write(carrierMonth, mean)
        })
      }

    }

  }

  def isActiveCarrier(vals: Iterable[Text]): Boolean = {
    vals.foreach(v => {
      if (v.toString.startsWith(OTPConsts.ACTIVE_YEAR)) {
        return true
      }
    })
    return false
  }

  def getMonth(value: Text): String = {
    var dates = value.toString.split(" ")(0).split("-")
    return dates(0) + "-" + dates(1)
  }

  def getPrice(value: Text): Double = {
    return value.toString.split(" ")(1).toDouble
  }

  def getMean(prices: List[Double]): Double = {
    var sum = 0.0
    prices.foreach(p => {
      sum += p
    })
    return sum / prices.length
  }
}
