
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//Find the (name, year) of the pitcher (appears in Pitching.csv) with the best batting average (Hits / At Bats).

// Author: Jun Cai
object BestBattingAverage {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Best Batting Average").
      setMaster("local")
    val sc = new SparkContext(conf)

    // Input
    val pitchers = sc.textFile("baseball/Pitching.csv").
      map {
        _.split(",")
      }.
      filter(a => {
        a(0) != "playerID" && a.length == 30
      }).
      map { _(0) }.
      distinct().
      collect()

    // calculate (Hits / At bat)
    val batting = sc.textFile("baseball/Batting.csv").
      map {
        _.split(",")
      }.
      filter(a => {
        a(0) != "playerID" && a.length == 22 && (pitchers contains a(0)) && !a(6).isEmpty && !a(8).isEmpty && a(6).toInt > 0
      }).
      map(a => {
        Array(a(1), a(0), a(6), a(8))
      }).
      keyBy(a => {
        a(0) + " " + a(1)
      }).
      mapValues(v => {
        Array(v(2).toInt, v(3).toInt)
      }).
      reduceByKey((v0, v1) => {
        Array(v0(0) + v1(0), v0(1) + v1(1))
      }).
      mapValues(v => {
        1.0 * v(1) / v(0)
      }).
      keyBy(r => {
        r._1.split(" ")(0)
      }).
      reduceByKey((v0, v1) => {
        if (v1._2 > v0._2) {
          v1
        } else {
          v0
        }
      }).
      sortByKey().
      mapValues(v => {
        val vals = v._1.split(" ");
        (vals(0), vals(1))
      })


    val text = batting.map { _._2.toString }
    text.saveAsTextFile("out")

    // Shut down Spark, avoid errors.
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
