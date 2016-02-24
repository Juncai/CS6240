package analysis

import java.text.SimpleDateFormat

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable

// Authors: Jun Cai and Vikas Boddu

object DataProcessor {

  val sf_dt = DateTimeFormat.forPattern(Consts.DATETIME_FORMAT)
  val sf_dk = DateTimeFormat.forPattern(Consts.DATEKEY_FORMAT)
  val sf_year = DateTimeFormat.forPattern(Consts.YEAR_FORMAT)

  def mapMissedConnection(kvs : (String, ((DateTime, DateTime), (DateTime, DateTime)))) : (String, Int) = {
    val (k, vs) = kvs
    val (arrTSs, depTSs) = vs
    val (arrScheduled, arrActual) = arrTSs
    val (depScheduled, depActual) = depTSs
    val carrier = k.split(",")(0)

    // check if it's a connection
    var numMissedCon = 0
    if (arrScheduled.plusMinutes(29).isBefore(depScheduled)
      && arrScheduled.plusMinutes(361).isAfter(depScheduled)) {
        if (arrActual.plusMinutes(30).isAfter(depActual)) {
          numMissedCon = 1
        }
    }
    (carrier + "," + getYearString(arrScheduled), numMissedCon)
  }

  def getYearString(dt : DateTime) : String = {
    dt.toString(sf_year)
  }

  def incTSInKeyByOneHour(kv : (String, (DateTime, DateTime))) : (String, (DateTime, DateTime)) = {
    val (k, v) = kv
    val kStrs = k.split(",")
    kStrs(2) = incOneHourToString(kStrs(2))
//    val dtStr = kStrs(2)
//    val newKey = kStrs(0) + "," + kStrs(1) + "," + incOneHourToString(kStrs(2))
//    (newKey, v)
    (kStrs.mkString(","), v)
  }

  def incOneHourToString(dtStr : String) : String = {
    val dt = sf_dk.parseDateTime(dtStr)
    dt.plusHours(1).toString(sf_dk)
//    dt.toString(sf_dk)
  }
  def flatMapFlights(flight : Array[String], isDep : Boolean) : Array[(String, (DateTime, DateTime))] = {
    val carrier = flight(Consts.F_CARRIER)
    val flDate = flight(Consts.F_FL_DATE)
    val originAirport = flight(Consts.F_O_AIRPORT)
    val destAirport = flight(Consts.F_D_AIRPORT)
    val depScheduledStr = flight(Consts.F_CRS_DEP)
    val depActualStr = flight(Consts.F_DEP)
    val arrScheduledStr = flight(Consts.F_CRS_ARR)
    val arrActualStr = flight(Consts.F_ARR)
    val depScheduled = getDateTime(flDate, depScheduledStr)
    var depActual = getDateTime(flDate, depActualStr)
    var arrScheduled = getDateTime(flDate, arrScheduledStr)
    var arrActual = getDateTime(flDate, arrActualStr)
    val depDelay = flight(Consts.F_DEP_DELAY).toDouble

    // handle cross day flights
    // bug fix: check if the flight depart earlier than scheduled
    if (depActual.isBefore(depScheduled) && depDelay > 0) depActual = depActual.plusDays(1)

    if (isDep) {
      Array((carrier + "," + originAirport + "," + dateToKey(depScheduled), (depScheduled, depActual)))
    } else {
      if (arrScheduled.isBefore(depScheduled)) arrScheduled = arrScheduled.plusDays(1)
      if (arrActual.isBefore(arrScheduled)) arrActual = arrActual.plusDays(1)

      val resBuffer = mutable.ArrayBuffer.empty[(String, (DateTime, DateTime))]
      val originTS = dateToKey(arrScheduled)
      val value = (arrScheduled, arrActual)
      var cTS = originTS
      var i = 0
      resBuffer += ((carrier + "," + destAirport + "," + cTS, value))
      for (i <- 0 until 6) {
        cTS = incOneHourToString(cTS)
        resBuffer += ((carrier + "," + destAirport + "," + cTS, value))
      }
      resBuffer.toArray
    }
  }

  def mapFlights(flight : Array[String], isDep : Boolean) : (String, (DateTime, DateTime)) = {
    val carrier = flight(Consts.F_CARRIER)
    val flDate = flight(Consts.F_FL_DATE)
    val originAirport = flight(Consts.F_O_AIRPORT)
    val destAirport = flight(Consts.F_D_AIRPORT)
    val depScheduledStr = flight(Consts.F_CRS_DEP)
    val depActualStr = flight(Consts.F_DEP)
    val arrScheduledStr = flight(Consts.F_CRS_ARR)
    val arrActualStr = flight(Consts.F_ARR)
    val depScheduled = getDateTime(flDate, depScheduledStr)
    var depActual = getDateTime(flDate, depActualStr)
    var arrScheduled = getDateTime(flDate, arrScheduledStr)
    var arrActual = getDateTime(flDate, arrActualStr)
    val depDelay = flight(Consts.F_DEP_DELAY).toDouble

    // handle cross day flights
    // bug fix: check if the flight depart earlier than scheduled
    if (depActual.isBefore(depScheduled) && depDelay > 0) depActual = depActual.plusDays(1)

    if (isDep) {
      (carrier + "," + originAirport + "," + dateToKey(depScheduled), (depScheduled, depActual))
    } else {
      if (arrScheduled.isBefore(depScheduled)) arrScheduled = arrScheduled.plusDays(1)
      if (arrActual.isBefore(arrScheduled)) arrActual = arrActual.plusDays(1)

      (carrier + "," + destAirport + "," + dateToKey(arrScheduled), (arrScheduled, arrActual))
    }
  }

  def dateToKey(d : DateTime) : String = {
    d.toString(sf_dk)
  }

  def getDateTime(dateStr : String , timeStr : String ) : DateTime = {
    // a hack to replace 2400
    if (timeStr.equals(Consts.START_OF_NEW_DAY_OLD)) {
      sf_dt.parseDateTime(dateStr + " " + Consts.START_OF_NEW_DAY).plusDays(1)
    } else {
      sf_dt.parseDateTime(dateStr + " " + timeStr)
    }
  }

  //static boolean sanityCheck(String[] values) {
  def sanityCheck(values: Array[String]): Boolean = {
    try {
      if (values.length != 110) return false

      // check not 0
      for (i <- Consts.NOTZERO) {
        if (values(i).toDouble == 0) return false
      }

      val timeZone = getMinDiff(values(Consts.CRS_ARR_TIME), values(Consts.CRS_DEP_TIME)) - values(Consts.CRS_ELAPSED_TIME).toDouble
      val residue = timeZone % 60
      if (residue != 0) return false


      // check larger than 0
      for (i <- Consts.LARGERTHANZERO) {
        if (values(i).toDouble <= 0) return false
      }

      // check not empty
      for (i <- Consts.NOTEMPTY) {
        if (values(i).isEmpty) return false
      }

      // for flights not canceled
      val isCanceled = values(Consts.CANCELLED).toDouble == 1

      // ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
      if (!isCanceled) {
        val timeDiff = getMinDiff(values(Consts.ARR_TIME), values(Consts.DEP_TIME)) - values(Consts.ACTUAL_ELAPSED_TIME).toDouble - timeZone
        if (timeDiff != 0) return false

        val arrDelay = values(Consts.ARR_DELAY).toDouble
        val arrDelayNew = values(Consts.ARR_DELAY_NEW).toDouble;
        // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
        if (arrDelay > 0) {
          if (arrDelay != arrDelayNew) return false
        }

        // if ArrDelay < 0 then ArrDelayMinutes???? should be zero
        if (arrDelay < 0) {
          if (arrDelayNew != 0) return false
        }
        // if ArrDelayMinutes >= 15 then ArrDel15 should be false
        val arrDel15 = values(Consts.ARR_DEL15).toDouble == 1
        if (arrDelayNew >= 15 && !arrDel15) return false
      }

      // finally, check the carrier field and price field
      val carrier = values(Consts.UNIQUE_CARRIER)
      if (carrier.isEmpty) return false
      val avgTicketPrice = values(Consts.AVG_TICKET_PRICE).toDouble
      val airTime = values(Consts.AIR_TIME)
      val airTimeVal = airTime.toDouble
      val distance = values(Consts.DISTANCE).toDouble

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
