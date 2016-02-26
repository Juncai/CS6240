package analysis

import org.joda.time.{DateTimeZone, DateTime, LocalDate}
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

  test("test DateTime/Long converter") {
    val dt = new DateTime()
    val l = DataProcessor.dateTimeToLong(dt)
    val c_dt = DataProcessor.longToDateTime(l)
    println(dt.withZone(DateTimeZone.UTC))
    println(l)
    println(c_dt)
  }
}
