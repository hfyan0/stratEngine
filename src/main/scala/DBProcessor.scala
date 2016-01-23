import java.sql.{Connection, DriverManager, ResultSet, Timestamp};
import java.util.Properties;
import scala.collection.mutable.ListBuffer

object DBProcessor {
  //--------------------------------------------------
  // mysql
  //--------------------------------------------------

  Class.forName("com.mysql.jdbc.Driver")

  var p = new Properties()
  p.put("user", Config.jdbcUser)
  p.put("password", Config.jdbcPwd)
  val _conn = DriverManager.getConnection(Config.jdbcConnStr, p)

  def deleteTradeTable() {
    try {
      val prep = _conn.prepareStatement("delete from trades")
      prep.executeUpdate
    }
  }

  def deleteMDItrdTable() {
    try {
      val prep = _conn.prepareStatement("delete from market_data_intraday")
      prep.executeUpdate
    }
  }

  def deleteItrdPnLTable() {
    try {
      val prep = _conn.prepareStatement("delete from intraday_pnl")
      prep.executeUpdate
    }
  }

  def insertTradeFeedToDB(sTradeFeed: String) {
    try {

      val (bIsTFValid, csvFields) = Util.parseAugmentedTradeFeed(sTradeFeed)
      if (bIsTFValid) {
        val prep = _conn.prepareStatement("insert into trades (timestamp,instrument_id,trade_volume,trade_price,buy_sell,order_id,strategy_id) values (?,?,?,?,?,?,?) ")

        prep.setString(1, Util.convertTimestampFmt1(csvFields(0)))
        prep.setString(2, csvFields(3).toString)
        prep.setDouble(3, csvFields(6).toDouble)
        prep.setDouble(4, csvFields(5).toDouble)
        prep.setDouble(5, csvFields(7).toDouble)
        prep.setInt(6, csvFields(9).toInt)
        prep.setString(7, csvFields(10))
        prep.executeUpdate
      }
    }
  }

  def batchInsertTradeFeedToDB(ltf: List[String]) {
    try {

      val prep = _conn.prepareStatement("insert into trades (timestamp,instrument_id,trade_volume,trade_price,buy_sell,order_id,strategy_id) values (?,?,?,?,?,?,?) ")

      ltf.foreach {
        tf =>
          val (bIsTFValid, csvFields) = Util.parseAugmentedTradeFeed(tf)
          if (bIsTFValid) {
            prep.setString(1, Util.convertTimestampFmt1(csvFields(0)))
            prep.setString(2, csvFields(3).toString)
            prep.setDouble(3, csvFields(6).toDouble)
            prep.setDouble(4, csvFields(5).toDouble)
            prep.setDouble(5, csvFields(7).toDouble)
            prep.setInt(6, csvFields(9).toInt)
            prep.setString(7, csvFields(10))
            prep.addBatch()
          }
      }

      prep.executeBatch

    }
  }

  def insertMarketDataToDB(sMarketFeed: String) {
    try {

      val (bIsMFValid, csvFields) = Util.parseMarketFeed(sMarketFeed)
      if (bIsMFValid) {
        val prep = _conn.prepareStatement("insert into market_data_intraday (timestamp,instrument_id,nominal_price) values (?,?,?)")

        prep.setString(1, Util.convertTimestampFmt1(csvFields(0)))
        prep.setString(2, csvFields(1).toString)
        prep.setDouble(3, csvFields(2).toDouble)
        prep.executeUpdate
      }
    }
  }

  def getLatestNominalPrices(): Map[String, Double] = {

    var results = scala.collection.mutable.Map[String, Double]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select instrument_id,nominal_price from market_data_intraday order by timestamp desc")

      while (rs.next) {
        results += rs.getString("instrument_id") -> rs.getDouble("nominal_price")
      }
    }
    results.toMap
  }

  def getTotalPnLOfSty(strategy_id: String): Double = {

    var results: Double = 0

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select sum(total_pnl) sum_total_pnl from intraday_pnl where strategy_id=?")
      prep.setString(1, strategy_id)
      val rs = prep.executeQuery()

      while (rs.next) {
        results = rs.getDouble("sum_total_pnl")
      }
    }
    results
  }
  def getLastPnLOfStySym(strategy_id: String, symbol: String): (Double, Double, Double) = {

    var rlzdPnL: Double = 0
    var urlzdPnL: Double = 0
    var totalPnL: Double = 0

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select realized_pnl,unrealized_pnl,total_pnl from intraday_pnl where strategy_id=? and instrument_id=? order by id desc limit 1")
      prep.setString(1, strategy_id)
      prep.setString(2, symbol)
      val rs = prep.executeQuery()

      while (rs.next) {
        rlzdPnL = rs.getDouble("realized_pnl")
        urlzdPnL = rs.getDouble("unrealized_pnl")
        totalPnL = rs.getDouble("total_pnl")
      }
    }
    (rlzdPnL, urlzdPnL, totalPnL)
  }

  def getAllStyFromTradesTable(): List[String] = {

    var results = ListBuffer[String]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select distinct strategy_id from trades order by strategy_id, timestamp")

      while (rs.next) {
        results += rs.getString("strategy_id")
      }
    }
    results.toList
  }

  def getAllTradesForSty(sty: String): List[TradeFeed] = {

    var results = ListBuffer[TradeFeed]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select instrument_id, trade_price, trade_volume, buy_sell from trades where strategy_id=? order by timestamp")
      prep.setString(1, sty)
      val rs = prep.executeQuery()

      while (rs.next) {
        val symbol = rs.getString("instrument_id")
        val trade_price = rs.getDouble("trade_price")
        val trade_volume = rs.getDouble("trade_volume")
        val trade_sign = if (rs.getInt("buy_sell") == 1) 1 else -1
        val signed_volume = trade_sign * trade_volume
        val signed_notional = trade_sign * trade_price * trade_volume

        val tf = TradeFeed(
          symbol,
          trade_price,
          trade_volume,
          trade_sign,
          signed_volume,
          signed_notional
        )
        results += tf
      }
    }
    results.toList
  }

  def insertPnLCalcRowToDB(pnlcalcrow: PnLCalcRow, strategy_id: String, symbol: String) {
    try {
      val prep = _conn.prepareStatement("insert into intraday_pnl (timestamp,instrument_id,realized_pnl,unrealized_pnl,total_pnl,position,strategy_id) values (?,?,?,?,?,?,?)")

      prep.setString(1, Util.getCurrentTimeStampStr)
      prep.setString(2, symbol)
      prep.setDouble(3, pnlcalcrow.cumRlzdPnL)
      prep.setDouble(4, pnlcalcrow.cumUrlzdPnL)
      prep.setDouble(5, pnlcalcrow.cumRlzdPnL + pnlcalcrow.cumUrlzdPnL)
      prep.setDouble(6, pnlcalcrow.cumSgndVol)
      prep.setString(7, strategy_id)
      prep.executeUpdate
    }
  }

  def closeConn(): Unit = {
    _conn.close
  }
}
