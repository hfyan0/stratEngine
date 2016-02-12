import org.nirvana._
import java.sql.{Connection, DriverManager, ResultSet, Timestamp};
import java.util.Properties;
import scala.collection.mutable.ListBuffer
import org.joda.time.{Period, DateTime, Duration}

object DBProcessor {
  //--------------------------------------------------
  // mysql
  //--------------------------------------------------

  Class.forName("com.mysql.jdbc.Driver")

  var p = new Properties()
  p.put("user", Config.jdbcUser)
  p.put("password", Config.jdbcPwd)
  val _conn = DriverManager.getConnection(Config.jdbcConnStr, p)
  // val _conn = DriverManager.getConnection(Config.jdbcConnStr + "?user=" +Config.jdbcUser+"&password="+Config.jdbcPwd)

  def deleteSignalsTable() {
    try {
      val prep = _conn.prepareStatement("delete from signals")
      prep.executeUpdate
    }
  }
  def deleteTradesTable() {
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
  def deletePortfolioTable() {
    try {
      val prep = _conn.prepareStatement("delete from portfolios")
      prep.executeUpdate
    }
  }

  def insertTradeFeedToDB(sTradeFeed: String) {
    try {

      val (bIsTFValid, csvFields) = Util.parseAugmentedTradeFeed(sTradeFeed)
      if (bIsTFValid) {
        {
          //--------------------------------------------------
          // trades table
          //--------------------------------------------------
          val prep = _conn.prepareStatement("insert into trades (timestamp,instrument_id,trade_volume,trade_price,buy_sell,order_id,strategy_id) values (?,?,?,?,?,?,?) ")
          prep.setString(1, Util.convertTimestampFmt1(csvFields(0))) // timestamp
          prep.setString(2, csvFields(3).toString) // instrument_id
          prep.setDouble(3, csvFields(6).toDouble) // trade_volume
          prep.setDouble(4, csvFields(5).toDouble) // trade_price
          prep.setDouble(5, csvFields(7).toDouble) // buy_sell
          prep.setInt(6, csvFields(9).toInt) // order_id
          prep.setString(7, csvFields(10)) // strategy_id
          prep.executeUpdate
        }
        {
          //--------------------------------------------------
          // signals table
          //--------------------------------------------------
          val prep = _conn.prepareStatement("insert into signals (status,timestamp,instrument_id,buy_sell,price,volume,comment,strat_id) values (?,?,?,?,?,?,?,?) ")
          prep.setInt(1, 0) // states
          prep.setString(2, Util.convertTimestampFmt1(csvFields(0))) //timestamp
          prep.setString(3, csvFields(3).toString) // instrument_id
          prep.setDouble(4, csvFields(7).toDouble) // buy_sell
          prep.setDouble(5, csvFields(5).toDouble) // price
          prep.setDouble(6, csvFields(6).toDouble) // volume
          prep.setString(7, csvFields(4)) // comment
          prep.setString(8, csvFields(10)) // strategy_id
          prep.executeUpdate
        }
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

  def insertMarketDataToItrdTbl(sMarketFeed: String) {
    try {

      val (bIsMFValid, mfnominal) = Util.parseMarketFeedNominal(sMarketFeed)
      if (bIsMFValid) {
        val prep = _conn.prepareStatement("insert into market_data_intraday (timestamp,instrument_id,nominal_price) values (?,?,?)")

        prep.setString(1, Util.convertDateTimeToStr(mfnominal.datetime))
        prep.setString(2, mfnominal.symbol)
        prep.setDouble(3, mfnominal.nominal_price)
        prep.executeUpdate
      }
    }
  }

  def insertMarketDataToHourlyTbl(sMarketFeed: String) {
    try {

      val (bIsMFValid, mfnominal) = Util.parseMarketFeedNominal(sMarketFeed)
      if (bIsMFValid) {
        val prep = _conn.prepareStatement("insert into market_data_hourly_hk_stock (timestamp,instrument_id,open,high,low,close,volume) values (?,?,?,?,?,?,?)")

        //--------------------------------------------------
        // TODO correct fake OHLC
        //--------------------------------------------------
        prep.setString(1, Util.convertDateTimeToStr(mfnominal.datetime))
        prep.setString(2, mfnominal.symbol)
        prep.setDouble(3, mfnominal.nominal_price)
        prep.setDouble(4, mfnominal.nominal_price)
        prep.setDouble(5, mfnominal.nominal_price)
        prep.setDouble(6, mfnominal.nominal_price)
        prep.setDouble(7, 1)
        prep.executeUpdate
      }
    }
  }
  def insertMarketDataToDailyTbl(sMarketFeed: String) {
    try {

      val (bIsMFValid, mfnominal) = Util.parseMarketFeedNominal(sMarketFeed)
      if (bIsMFValid) {
        val prep = _conn.prepareStatement("insert into market_data_daily_hk_stock (timestamp,instrument_id,open,high,low,close,volume) values (?,?,?,?,?,?,?)")

        //--------------------------------------------------
        // TODO correct fake OHLC
        //--------------------------------------------------
        prep.setString(1, Util.convertDateTimeToStr(mfnominal.datetime))
        prep.setString(2, mfnominal.symbol)
        prep.setDouble(3, mfnominal.nominal_price)
        prep.setDouble(4, mfnominal.nominal_price)
        prep.setDouble(5, mfnominal.nominal_price)
        prep.setDouble(6, mfnominal.nominal_price)
        prep.setDouble(7, 1)
        prep.executeUpdate
      }
    }
  }

  def getNominalPricesAsAt(asOfDate: DateTime): Map[String, Double] = {

    var results = List[(String, DateTime, Double)]()
    var symbols = Set[String]()
    var res_map = Map[String, Double]()

    //--------------------------------------------------
    // from intraday table
    //--------------------------------------------------
    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select timestamp,instrument_id,nominal_price from market_data_intraday order by timestamp desc")

      while (rs.next) {

        val symbol = rs.getString("instrument_id")
        val datetime = Util.convertMySQLTSToDateTime(rs.getString("timestamp"))
        val nominal_price = rs.getDouble("nominal_price")

        symbols += symbol
        results ::= (symbol, datetime, nominal_price)
      }
    }
    //--------------------------------------------------
    // from hourly table
    //--------------------------------------------------
    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select timestamp,instrument_id,close from market_data_hourly_hk_stock order by timestamp desc")

      while (rs.next) {

        val symbol = rs.getString("instrument_id")
        val datetime = Util.convertMySQLTSToDateTime(rs.getString("timestamp"))
        val close = rs.getDouble("close")

        symbols += symbol
        results ::= (symbol, datetime, close)
      }
    }
    //--------------------------------------------------
    // from daily table
    //--------------------------------------------------
    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select timestamp,instrument_id,close from market_data_daily_hk_stock order by timestamp desc")

      while (rs.next) {

        val symbol = rs.getString("instrument_id")
        val datetime = Util.convertMySQLTSToDateTime(rs.getString("timestamp"))
        val close = rs.getDouble("close")

        symbols += symbol
        results ::= (symbol, datetime, close)
      }
    }

    //--------------------------------------------------
    // sort all the obtained prices
    //--------------------------------------------------
    val res_1 = results.filter(_._2.getMillis <= asOfDate.getMillis)

    val res_list = symbols.map(sym => res_1.filter(_._1 == sym).sortWith(_._2.getMillis > _._2.getMillis) match {
      case Nil     => (sym, new DateTime(), 0.0)
      case x :: xs => x
    })
    res_list.foreach(t => res_map += (t._1 -> t._3))

    res_map
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
  def getLastDateTimeInDailyPnLTbl(): Option[DateTime] = {
    var results: Option[DateTime] = None

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select timestamp from daily_pnl order by timestamp desc limit 1")

      while (rs.next) {
        results = Some(Util.convertMySQLTSToDateTime(rs.getString("timestamp")))
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

      val prep = _conn.prepareStatement("select timestamp,instrument_id,trade_price,trade_volume,buy_sell from trades where strategy_id=? order by timestamp")
      prep.setString(1, sty)
      val rs = prep.executeQuery()

      while (rs.next) {
        val datetime = Util.convertMySQLTSToDateTime(rs.getString("timestamp"))
        val symbol = rs.getString("instrument_id")
        val trade_price = rs.getDouble("trade_price")
        val trade_volume = rs.getDouble("trade_volume")
        val trade_sign = if (rs.getInt("buy_sell") == 1) 1 else -1
        val signed_volume = trade_sign * trade_volume
        val signed_notional = trade_sign * trade_price * trade_volume

        val tf = TradeFeed(
          datetime,
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

  def insertPnLCalcRowToItrdPnLTbl(strategy_id: String, symbol: String, pnlcalcrow: PnLCalcRow) {
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

  def insertPnLCalcRowToDailyPnLTbl(dt: Option[DateTime], strategy_id: String, symbol: String, pnlcalcrow: PnLCalcRow) {
    try {
      val prep = _conn.prepareStatement("insert into daily_pnl (timestamp,instrument_id,realized_pnl,unrealized_pnl,position,strategy_id) values (?,?,?,?,?,?)")

      val dtToUse = dt match {
        case Some(dt: DateTime) => dt
        case _                  => Util.getCurrentDateTime
      }

      prep.setString(1, Util.convertDateTimeToStr(dtToUse))
      prep.setString(2, symbol)
      prep.setDouble(3, pnlcalcrow.cumRlzdPnL)
      prep.setDouble(4, pnlcalcrow.cumUrlzdPnL)
      prep.setDouble(5, pnlcalcrow.cumSgndVol)
      prep.setString(6, strategy_id)
      prep.executeUpdate
    }
  }

  def insertPortfolioTbl(dt: Option[DateTime], strategy_id: String, symbol: String, signedPos: Double, avgPx: Double) {
    try {
      val prep = _conn.prepareStatement("insert into portfolios (instrument_id,volume,avg_price,timestamp,strat_id) values (?,?,?,?,?)")

      val dtToUse = dt match {
        case Some(dt: DateTime) => dt
        case _                  => Util.getCurrentDateTime
      }
      prep.setString(1, symbol)
      prep.setDouble(2, signedPos)
      prep.setDouble(3, avgPx)
      prep.setString(4, Util.convertDateTimeToStr(dtToUse))
      prep.setString(5, strategy_id)
      prep.executeUpdate
    }
  }

  def getAllTradesOfSty(sid: String): List[TradeFeed] = {

    var results = ListBuffer[TradeFeed]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select timestamp,instrument_id,trade_price,trade_volume,buy_sell from trades where strategy_id=?")
      prep.setString(1, sid)
      val rs = prep.executeQuery

      while (rs.next) {
        val datetime = Util.convertMySQLTSToDateTime(rs.getString("timestamp"))
        val symbol = rs.getString("instrument_id")
        val trade_price = rs.getDouble("trade_price")
        val trade_volume = rs.getDouble("trade_volume")
        val trade_sign = rs.getInt("buy_sell")
        val signed_volume = trade_volume * { if (trade_sign == 1) 1 else -1 }
        val signed_notional = trade_price * signed_volume

        results += TradeFeed(
          datetime,
          symbol,
          trade_price,
          trade_volume,
          trade_sign,
          signed_volume,
          signed_notional
        )

      }
    }
    results.toList
  }

  def closeConn(): Unit = {
    _conn.close
  }
}
