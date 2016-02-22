package analysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


// Author: Jun Cai and Vikas Boddu
object MissedConnections {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Missed Connections Analysis").
      setMaster("local") // for testing locally
    val sc = new SparkContext(conf)

    // Input
//    val flights = sc.textFile(args(0)).
    val flights = sc.textFile("/home/phoenix/Downloads/part").
      map (DataProcessor.parseCSVLine).
      filter(DataProcessor.sanityCheck).
      map (r => Array(r(OTPConsts.UNIQUE_CARRIER), r(OTPConsts.FL_DATE), r(OTPConsts.ORIGIN_AIRPORT_ID),
        r(OTPConsts.CRS_DEP_TIME), r(OTPConsts.DEP_TIME), r(OTPConsts.DEST_AIRPORT_ID),
        r(OTPConsts.CRS_ARR_TIME), r(OTPConsts.ARR_TIME)))

    val depFlight = flights.
      map(r => ())
//    // calculate (Hits / At bat)
//    val batting = sc.textFile("baseball/Batting.csv").
//      map {
//        _.split(",")
//      }.
//      filter(a => {
//        a(0) != "playerID" && a.length == 22 && (pitchers contains a(0)) && !a(6).isEmpty && !a(8).isEmpty && a(6).toInt > 0
//      }).
//      map(a => {
//        Array(a(1), a(0), a(6), a(8))
//      }).
//      keyBy(a => {
//        a(0) + " " + a(1)
//      }).
//      mapValues(v => {
//        Array(v(2).toInt, v(3).toInt)
//      }).
//      reduceByKey((v0, v1) => {
//        Array(v0(0) + v1(0), v0(1) + v1(1))
//      }).
//      mapValues(v => {
//        1.0 * v(1) / v(0)
//      }).
//      keyBy(r => {
//        r._1.split(" ")(0)
//      }).
//      reduceByKey((v0, v1) => {
//        if (v1._2 > v0._2) {
//          v1
//        } else {
//          v0
//        }
//      }).
//      sortByKey().
//      mapValues(v => {
//        val vals = v._1.split(" ");
//        (vals(0), vals(1))
//      })
//
//    flights.saveAsTextFile(args(1))
    flights.saveAsTextFile("out")
//    val text = batting.map { _._2.toString }
//    text.saveAsTextFile(args(1))

    // Shut down Spark, avoid errors.
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
