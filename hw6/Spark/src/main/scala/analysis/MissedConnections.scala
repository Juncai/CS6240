package analysis

import org.apache.spark._


// Author: Jun Cai and Vikas Boddu
object MissedConnections {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Missed Connections Analysis")
    if (args.length == 3) {
      conf.setMaster("local") // for running locally
    }
    val sc = new SparkContext(conf)

    // Input
    val flights = sc.textFile(args(0)).
//    val flights = sc.textFile("/home/jon/Downloads/OTP/part").
      map (DataProcessor.parseCSVLine).
//      filter(DataProcessor.sanityCheck).
      map (r => Array(r(Consts.UNIQUE_CARRIER), r(Consts.FL_DATE), r(Consts.ORIGIN_AIRPORT_ID),
        r(Consts.CRS_DEP_TIME), r(Consts.DEP_TIME), r(Consts.DEST_AIRPORT_ID),
        r(Consts.CRS_ARR_TIME), r(Consts.ARR_TIME), r(Consts.DEP_DELAY)))

    // key = {carrier, airport_id, scheduled_dep/arr_in_hour_precision}, value = {scheduled_dep/arr, actual_dep/arr}
    val depFlight = flights.
      map(DataProcessor.mapFlights(_, isDep = true))
//      cache()

    val arrFlight = flights.
//      map(DataProcessor.mapFlights(_, isDep = false)).
      flatMap(DataProcessor.flatMapFlights(_, isDep = false))
//      cache()

    val connections = arrFlight.
      join(depFlight).
      map(DataProcessor.mapMissedConnection).
      reduceByKey(_ + _)

//    var i = 0
//    for (i <- 0 until 6) {
//      val newArr = arrFlight
//        .map(DataProcessor.incTSInKeyByOneHour)
//      val newConnections = newArr.join(depFlight)
//      connections.++(newConnections)
//    }

//    val res = connections.
//      map(DataProcessor.mapMissedConnection).
//      reduceByKey(_ + _)

//    res.saveAsTextFile(args(1))
    connections.saveAsTextFile(args(1))
//    res.saveAsTextFile("out")

    // Shut down Spark, avoid errors
    sc.stop()
  }
}

// vim: set ts=4 sw=4 et:
