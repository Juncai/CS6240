package analysis

import java.text.SimpleDateFormat

import org.joda.time.DateTime

import scala.collection.mutable

// Authors: Jun Cai and Vikas Boddu

object DataProcessor {

  def mapFlights(flight : Array[String], isDep : Boolean) : Tuple2[String, Array[DateTime]] = {

  }

  //static boolean sanityCheck(String[] values) {
  def sanityCheck(values: Array[String]): Boolean = {
    try {
      if (values.length != 110) return false

      // check not 0
      for (i <- OTPConsts.NOTZERO) {
        if (values(i).toDouble == 0) return false
      }

      val timeZone = getMinDiff(values(OTPConsts.CRS_ARR_TIME), values(OTPConsts.CRS_DEP_TIME)) - values(OTPConsts.CRS_ELAPSED_TIME).toDouble
      val residue = timeZone % 60
      if (residue != 0) return false


      // check larger than 0
      for (i <- OTPConsts.LARGERTHANZERO) {
        if (values(i).toDouble <= 0) return false
      }

      // check not empty
      for (i <- OTPConsts.NOTEMPTY) {
        if (values(i).isEmpty) return false
      }

      // for flights not canceled
      val isCanceled = values(OTPConsts.CANCELLED).toDouble == 1

      // ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
      if (!isCanceled) {
        val timeDiff = getMinDiff(values(OTPConsts.ARR_TIME), values(OTPConsts.DEP_TIME)) - values(OTPConsts.ACTUAL_ELAPSED_TIME).toDouble - timeZone
        if (timeDiff != 0) return false

        val arrDelay = values(OTPConsts.ARR_DELAY).toDouble
        val arrDelayNew = values(OTPConsts.ARR_DELAY_NEW).toDouble;
        // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
        if (arrDelay > 0) {
          if (arrDelay != arrDelayNew) return false
        }

        // if ArrDelay < 0 then ArrDelayMinutes???? should be zero
        if (arrDelay < 0) {
          if (arrDelayNew != 0) return false
        }
        // if ArrDelayMinutes >= 15 then ArrDel15 should be false
        val arrDel15 = values(OTPConsts.ARR_DEL15).toDouble == 1
        if (arrDelayNew >= 15 && !arrDel15) return false
      }

      // finally, check the carrier field and price field
      val carrier = values(OTPConsts.UNIQUE_CARRIER)
      if (carrier.isEmpty) return false
      val avgTicketPrice = values(OTPConsts.AVG_TICKET_PRICE).toDouble
      val airTime = values(OTPConsts.AIR_TIME)
      val airTimeVal = airTime.toDouble
      val distance = values(OTPConsts.DISTANCE).toDouble

    } catch {
      case e : Exception => return false
    }

    true
  }

  def getMinDiff(t1: String, t2: String): Int = {
    val format = new SimpleDateFormat("HHmm")
    val date1 = format.parse(t1)
    val date2 = format.parse(t2)
    var timeDiff = (date1.getTime() - date2.getTime()) / 60000
    if (timeDiff <= 0) {
      timeDiff += 24 * 60
    }

    timeDiff.toInt
  }



  def parseCSVLine(line : String) : Array[String] = {
    val values = mutable.ArrayBuffer.empty[String]
    var sb = new StringBuffer()
    var inQuote = false
      for (c <- line) {
      if (inQuote) {
        if (c == '"') {
          inQuote = false
        } else {
          sb.append(c)
        }
      } else {
        if (c == '"') {
          inQuote = true
        } else if (c == ',') {
          values += sb.toString
          sb = new StringBuffer()
        } else {
          sb.append(c)
        }
      }
    }
    values += sb.toString  // last field

    values.toArray
  }

}
