case class TradeFeed(
  val symbol:          String,
  val trade_price:     Double,
  val trade_volume:    Double,
  val trade_sign:      Int,
  val signed_volume:   Double,
  val signed_notional: Double
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
  def getCurrentTimeStamp(): java.sql.Timestamp =
    {
      val today = new java.util.Date()
      new java.sql.Timestamp(today.getTime())
    }
  def getCurrentTimeStampStr(): String =
    {
      getCurrentTimeStamp.toString
    }

  //--------------------------------------------------
  // from 20160112_123456_000000 to 2016-01-12 12:34:56.000000
  //--------------------------------------------------
  def convertTimestampFmt1(ts: String) =
    {
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

  def parseTradeFeed(sTradeFeed: String): (Boolean, List[String]) =
    {
      val csvFields = sTradeFeed.split(",")
      if (csvFields.length == 10) (true, csvFields.toList)
      else (false, List())
    }
  def parseAugmentedTradeFeed(sTradeFeed: String): (Boolean, List[String]) =
    {
      val csvFields = sTradeFeed.split(",")
      if (csvFields.length == 11) (true, csvFields.toList)
      else (false, List())
    }
  def parseMarketFeed(sMarketFeed: String): (Boolean, List[String]) =
    {
      val csvFields = sMarketFeed.split(",")
      if (csvFields.length == 3 && isDouble(csvFields(2))) (true, csvFields.toList)
      else (false, List())
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

}
