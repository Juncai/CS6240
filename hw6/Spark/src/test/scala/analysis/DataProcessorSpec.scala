package analysis

import org.joda.time.{DateTime, LocalDate}
import org.scalatest._
/**
  * Created by phoenix on 2/21/16.
  */
class DataProcessorSpec extends FunSuite {

  test("parse CSV line") {
    val line = """2013,1,1,17,4,2013-01-17,"9E",20363,"9E","N923XJ","3324",11298,1129802,30194,"DFW","Dallas/Fort Worth, TX","TX","48","Texas",74,12478,1247801,31703,"JFK","New York, NY","NY","36","New York",22,"1045","1038",-7.00,0.00,0.00,-1,"1000-1059",10.00,"1048","1443",8.00,"1505","1451",-14.00,0.00,0.00,-1,"1500-1559",0,"",0,200,193,175,1,1391,6,,,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,668.0"""
    val res = DataProcessor.parseCSVLine(line)
    val valid = DataProcessor.sanityCheck(res)
    if (valid) println("valid line")
  }

  test("increment hour in key") {
    val kv = ("F9,12345,2005010100", (new DateTime(), new DateTime()))
    val hourNew = DataProcessor.incOneHourToString("2005010100")
    val kvNew = DataProcessor.incTSInKeyByOneHour(kv)
    println(hourNew)
    println(kvNew._1)
  }

  test("date key related test") {
    val current = new DateTime()
    val dKey = DataProcessor.dateToKey(current)
    val dKeyPlusOneHour = DataProcessor.incOneHourToString(dKey)
    println(current.toString())
    println(dKey)
    println(dKeyPlusOneHour)
  }

  test("test missed connection check") {
    val arrScheduled = new DateTime().plusHours(10).plusMinutes(10).minusDays(55)
    val depScheduled = arrScheduled.plusMinutes(30)
    val arrActual = arrScheduled.plusMinutes(20)
    val depActual = arrScheduled.plusMinutes(50)
    val key = "E8,12345,2016"
    val isMissed = DataProcessor.mapMissedConnection((key, ((arrScheduled, arrActual), (depScheduled, depActual))))
    println(isMissed)
  }

  test("test parse flight info") {
    val str = """2012,1,2,20,3,2012-12-31,9E,20363,9E,N904XJ,3322,13244,1324401,33244,MEM,"Memphis, TN",TN,47,Tennessee,54,10721,1072101,30721,BOS,"Boston, MA",MA,25,Massachusetts,13,0825,0819,-6,0,0,-1,0800-0859,15,0834,1159,7,2350,0306,14,0,0,-1,1200-1259,0,,0,175,167,145,1,1139,5,,,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,434"""
    val r = DataProcessor.parseCSVLine(str)
    val info = Array(r(Consts.UNIQUE_CARRIER), r(Consts.FL_DATE), r(Consts.ORIGIN_AIRPORT_ID),
        r(Consts.CRS_DEP_TIME), r(Consts.DEP_TIME), r(Consts.DEST_AIRPORT_ID),
        r(Consts.CRS_ARR_TIME), r(Consts.ARR_TIME), r(Consts.DEP_DELAY))
    val f = DataProcessor.flatMapFlights(info, isDep = false)
    println(f)
  }
}
