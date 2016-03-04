import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting


// Author: Jun Cai
object Indexing {
  val PLAYER_ID = 0
  val YEAR_ID = 1
  val TEAM_ID = 3
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Most HR Per Dollar").
      setMaster("local")
    val sc = new SparkContext(conf)

    // Building index
    val batting = sc.textFile("baseball/Batting.csv").
      map {
        _.split(",")
      }.
      filter(a => {
        a(PLAYER_ID) != "playerID"
      }).
      map(a => {
        val ptMap : mutable.Map[String, ArrayBuffer[String]] = mutable.Map()
        ptMap.getOrElseUpdate(a(PLAYER_ID), new ArrayBuffer[String]()) += a(YEAR_ID) + " " + a(TEAM_ID)
        ("MAP", ptMap)
      }).
      reduceByKey((m0, m1) => {
        m1.keys.foreach(pid => {
          m0.getOrElseUpdate(pid, new ArrayBuffer[String]()) += m1.get(pid).get(0)
        })
        m0
      }).
      mapValues(m => {
        val nm : mutable.Map[String, Array[String]] = mutable.Map()
        m.keys.foreach(pid => {
          val cArray : Array[String] = m.apply(pid).toArray
          Sorting.quickSort(cArray)
          nm.update(pid, cArray)
        })
        nm
      }).
      coalesce(1).
      saveAsObjectFile("out")

    // Shut down Spark, avoid errors.
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
