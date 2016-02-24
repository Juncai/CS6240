package analysis

// Author: Jun Cai and Vikas Boddu
object Consts {
  // flight string array indices
  val F_CARRIER = 0
  val F_FL_DATE = 1
  val F_O_AIRPORT = 2
  val F_CRS_DEP = 3
  val F_DEP = 4
  val F_D_AIRPORT = 5
  val F_CRS_ARR = 6
  val F_ARR = 7
  val F_DEP_DELAY = 8

  // 2400
  val START_OF_NEW_DAY_OLD = "2400"
  val START_OF_NEW_DAY = "0000"

  // Header start with
  val HEADER_START = "YEAR"

  // Date time format
  val DATETIME_FORMAT = "yyyy-MM-dd HHmm"
  val DATEKEY_FORMAT = "yyyyMMddHH"
  val YEAR_FORMAT = "yyyy"

  // for flight time and distance
  val DISTANCE = 54
  val AIR_TIME = 52

  // constants for indices
  val YEAR = 0
  val FL_DATE = 5
  val CRS_ARR_TIME = 40
  val CRS_DEP_TIME = 29
  val CRS_ELAPSED_TIME = 50
  val NOTZERO = Array(29, 40)

  // ORIGIN_AIRPORT_ID 11
  val ORIGIN_AIRPORT_ID = 11
  // DEST_AIRPORT_ID 20 (do we need DIV_AIRPORT_ID?)
  val DEST_AIRPORT_ID = 20


  // ORIGIN_AIRPORT_ID 11
  // ORIGIN_AIRPORT_SEQ_ID 12
  // ORIGIN_CITY_MARKET_ID 13
  // ORIGIN_STATE_FIPS 17
  // ORIGIN_WAC 19
  // DEST_AIRPORT_ID 20 (do we need DIV_AIRPORT_ID?)
  // DEST_AIRPORT_SEQ_ID 21
  // DEST_CITY_MARKET_ID 22
  // DEST_STATE_FIPS 26
  // DEST_WAC 28
  val LARGERTHANZERO = Array(11, 12, 13, 17, 19, 20, 21, 22, 26, 28)

  // ORIGIN 14
  // ORIGIN_CITY_NAME 15
  // ORIGIN_STATE_ABR 16
  // ORIGIN_STATE_NM 18
  // DEST 23
  // DEST_CITY_NAME 24
  // DEST_STATE_ABR 25
  // DEST_STATE_NM 27
  val NOTEMPTY = Array(14, 15, 16, 18, 23, 24, 25, 27)

  val CANCELLED = 47
  val DEP_TIME = 30
  val ARR_TIME = 41
  val ACTUAL_ELAPSED_TIME = 51
  val ARR_DELAY = 42
  val DEP_DELAY = 31
  val ARR_DELAY_NEW = 43
  val ARR_DEL15 = 44
  val UNIQUE_CARRIER = 6
  val AVG_TICKET_PRICE = 109
}
