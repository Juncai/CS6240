
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

//Write a Spark program that finds the (player, year) that hit the most home runs per dollar of salary.

// Author: Jun Cai
object Query {
  val PLAYER_ID = 0
  val YEAR_ID = 1
  val TEAM_ID = 3
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Most HR Per Dollar").
      setMaster("local")
    val sc = new SparkContext(conf)
//    val indexPath = args(0)
    val indexPath = "index"
//    val playerID = args(1)
    val playerID = "bowlebr01"

    // Input
    val index = sc.objectFile[(String, mutable.Map[String, Array[String]])](indexPath).
      flatMap(r => {
        val (_, m) = r
        m.apply(playerID)
      }).
//      saveAsTextFile(args(2))
      saveAsTextFile("out")

    // Shut down Spark, avoid errors.
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
