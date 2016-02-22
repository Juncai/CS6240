package analysis

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

}
