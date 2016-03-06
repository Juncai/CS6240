package fastquery

import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable


// Author: Jun Cai
object Query {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Fast Query").
      setMaster("local")
    val sc = new SparkContext(conf)
    val indexPath = args(0)
    val playerID = args(1)

    // load index
    val index = sc.objectFile[(String, mutable.Map[String, Array[String]])](indexPath).
      map(_._2).
      reduce((m0, m1) => {
        m1
      })

    println("#############################")
    println("Query results for " + playerID)
    val startTS = new Date().getTime()
    index.apply(playerID).foreach(println(_))

    println("Time used: " + (new Date().getTime() - startTS) + " ms")
    println("#############################")

    // Shut down Spark, avoid errors.
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
