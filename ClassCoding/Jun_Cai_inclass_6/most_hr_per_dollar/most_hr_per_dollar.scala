
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//Write a Spark program that finds the (player, year) that hit the most home runs per dollar of salary.

// Author: Jun Cai
object MostHRPerDollar {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Most HR Per Dollar").
      setMaster("local")
    val sc = new SparkContext(conf)

    // Input
    val batting = sc.textFile("baseball/Batting.csv").
      map {
        _.split(",")
      }.
      filter(a => {
        a(0) != "playerID" && a.length == 22 && !a(11).isEmpty
      }).
      map(a => {
        Array(a(1), a(0), a(11))
      }).
      keyBy(a => {
        a(0) + " " + a(1)
      })

    val salaries = sc.textFile("baseball/Salaries.csv").
      map {
        _.split(",")
      }.
      filter(a => {
        a(0) != "yearID" && a.length == 5 && !a(4).isEmpty && a(4).toDouble > 0
      }).
      map(a => {
        Array(a(0), a(3), a(4))
      }).
      keyBy(a => {
        a(0) + " " + a(1)
      })


    // Join
    val data = batting.join(salaries)

    // calculate the HR per Dollar
    val hr_per_d = data.
      mapValues(v => {
        val (b, s) = v;
        Array(b(2).toDouble, s(2).toDouble)
      }).
      reduceByKey((v0, v1) => {
        Array(v0(0) + v1(0), v0(1) + v1(1))
      }).
      mapValues(v => {
        v(0) / v(1)
      }).
      keyBy(r => {
        r._1.split(" ")(0).toInt
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

    val text = hr_per_d.map { _._2.toString }
    text.saveAsTextFile("out")

    // Shut down Spark, avoid errors.
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
