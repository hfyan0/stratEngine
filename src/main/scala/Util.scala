import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Period, DateTime, Duration, Days}

case class TradeFeed(
  val datetime:        DateTime,
  val symbol:          String,
  val trade_price:     Double,
  val trade_volume:    Double,
  val trade_sign:      Int,
  val signed_volume:   Double,
  val signed_notional: Double
)

case class MarketFeedNominal(
  val datetime:      DateTime,
  val symbol:        String,
  val nominal_price: Double
)

case class PnLCalcRow(
  val trdPx:                Double,
  val trdVol:               Double,
  val trdSgn:               Int,
  val cumSgndVol:           Double,
  val cumSgndNotlAdjChgSgn: Double,
  val avgPx:                Double,
  val prdTotPnL:            Double,
  val cumTotPnL:            Double,
  val psnClosed:            Double,
  val rlzdPnL:              Double,
  val cumRlzdPnL:           Double,
  val cumUrlzdPnL:          Double
)

object Util {
  val EPSILON = 0.00001
  val SMALLNUM = 0.01
  val EPOCH = new DateTime(1970, 1, 1, 0, 0, 0)

  private val _dateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def getCurrentTimeStamp(): java.sql.Timestamp = {
    val today = new java.util.Date()
    new java.sql.Timestamp(today.getTime())
  }
  def getCurrentTimeStampStr(): String = {
    getCurrentTimeStamp.toString
  }
  def getCurrentDateTime(): DateTime = {
    new DateTime()
  }
  def getCurrentDateTimeStr(): String = {
    getCurrentDateTime.toString
  }

  def convertDateTimeToStr(dt: DateTime): String = {
    _dateTimeFormat.print(dt)
  }
  //--------------------------------------------------
  // from 20160112_123456_000000 to 2016-01-12 12:34:56.000000
  //--------------------------------------------------
  def convertTimestampFmt1(ts: String): String = {
    val tsFields = ts.split("_")
    if (tsFields.length != 3) ""
    else {
      val sYYYYMMDD = tsFields(0)
      val sHHMMSS = tsFields(1)
      val sMillisec = tsFields(2)

      val sYear = sYYYYMMDD.substring(0, 4)
      val sMonth = sYYYYMMDD.substring(4, 6)
      val sDay = sYYYYMMDD.substring(6, 8)

      val sHour = sHHMMSS.substring(0, 2)
      val sMin = sHHMMSS.substring(2, 4)
      val sSec = sHHMMSS.substring(4, 6)

      sYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin + ":" + sSec + "." + sMillisec
    }
  }

  //--------------------------------------------------
  // from MySQL timestamp in the format 
  //--------------------------------------------------
  def convertMySQLTSToDateTime(ts: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dt: DateTime = formatter.parseDateTime(ts.substring(0, 19))
    dt
  }

  def convertTimestampFmt2(ts: String): DateTime = {
    Util.convertMySQLTSToDateTime(Util.convertTimestampFmt1(ts))
  }
  def parseTradeFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",")
    if (csvFields.length == 10) (true, csvFields.toList)
    else (false, List())
  }
  def parseAugmentedTradeFeed(sTradeFeed: String): (Boolean, List[String]) = {
    val csvFields = sTradeFeed.split(",")
    if (csvFields.length == 11) (true, csvFields.toList)
    else (false, List())
  }
  def parseMarketFeedNominal(sMarketFeed: String): (Boolean, MarketFeedNominal) = {
    val csvFields = sMarketFeed.split(",")
    if (csvFields.length == 3 && isDouble(csvFields(2))) (true, MarketFeedNominal(Util.convertTimestampFmt2(csvFields(0)), csvFields(1), csvFields(2).toDouble))
    else (false, MarketFeedNominal(Util.EPOCH, "", 0))
  }

  def isDouble(x: String) = {
    try {
      x.toDouble
      true
    }
    catch {
      case e: NumberFormatException => false
    }
  }

def getListOfDatesWithinRange(startDT: DateTime, endDT: DateTime): List[DateTime] = {
val numofdays = Days.daysBetween(startDT, endDT).getDays()
(0 until numofdays).map(startDT.plusDays(_)).toList
}


}

class PeriodicTask(val intervalInSec: Int) {
  var _lastTime: DateTime = Util.EPOCH
  def checkIfItIsTimeToWakeUp(timeNow: DateTime): Boolean = {
    val msAfter = timeNow.getMillis - _lastTime.getMillis
    val secAfter = msAfter / 1000
    if (secAfter >= intervalInSec) {
      _lastTime = _lastTime.plusSeconds((secAfter / intervalInSec).toInt * intervalInSec)
      true
    }
    else false

  }
}
