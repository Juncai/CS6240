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
    val flights = sc.textFile("/home/jon/Downloads/OTP/part").
      map (DataProcessor.parseCSVLine).
      filter(DataProcessor.sanityCheck).
      map (r => Array(r(Consts.UNIQUE_CARRIER), r(Consts.FL_DATE), r(Consts.ORIGIN_AIRPORT_ID),
        r(Consts.CRS_DEP_TIME), r(Consts.DEP_TIME), r(Consts.DEST_AIRPORT_ID),
        r(Consts.CRS_ARR_TIME), r(Consts.ARR_TIME)))

    // key = {carrier, airport_id, scheduled_dep/arr_in_hour_precision}, value = {scheduled_dep/arr, actual_dep/arr}
    val depFlight = flights.
      map(DataProcessor.mapFlights(_, isDep = true))

    val arrFlight = flights.
      map(DataProcessor.mapFlights(_, isDep = false))

    val connections = arrFlight.join(depFlight)

    var i = 0
    for (i <- 0 until 6) {
      val newArr = arrFlight
        .map(DataProcessor.incTSInKeyByOneHour)
      val newConnections = newArr.join(depFlight)
      connections.++(newConnections)
    }

    val res = connections.
      map(DataProcessor.mapMissedConnection).
      reduceByKey(_ + _)

//    res.saveAsTextFile(args(1))
    res.saveAsTextFile("out")

    // Shut down Spark, avoid errors
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
