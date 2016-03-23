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

  var lsConn = List[Connection]()

  (0 until Config.jdbcConnStr.length).foreach(i => {
    var p = new Properties()
    p.put("user", Config.jdbcUser(i))
    p.put("password", Config.jdbcPwd(i))
    val _conn = DriverManager.getConnection(Config.jdbcConnStr(i), p)
    _conn.setAutoCommit(false)
    lsConn ::= _conn
  })

  def deleteSignalsTable() {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from signals")
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def deleteTradesTable() {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from trades")
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def deleteMDItrdTable() {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from market_data_intraday")
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def deleteItrdPnLTable() {
    lsConn.foreach(_conn => {
      try {
        val prep1 = _conn.prepareStatement("delete from intraday_pnl")
        prep1.executeUpdate
        val prep2 = _conn.prepareStatement("delete from intraday_pnl_per_strategy")
        prep2.executeUpdate
        _conn.commit
      }
    })
  }

  def deletePortfolioTable() {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from portfolios")
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def deletePortfolioTable(lsStySymPnLRow: List[(String, String, PnLCalcRow)]) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from portfolios where strategy_id = ? and instrument_id = ?")

        lsStySymPnLRow.foreach {
          case (stratid, symbol, pnlcalcrow) => {
            prep.setString(1, stratid)
            prep.setString(2, symbol)
            prep.addBatch
          }
        }

        prep.executeBatch
        _conn.commit
      }
    })
  }

  def deletePortfolioTableWithStratIDSym(stratid: String, symbol: String) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from portfolios where strategy_id = ? and instrument_id = ?")
        prep.setString(1, stratid)
        prep.setString(2, symbol)
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def insertTradeFeedToDB(sTradeFeed: String) {
    lsConn.foreach(_conn => {
      try {
        val (bIsTFValid, csvFields) = SUtil.parseAugmentedTradeFeed(sTradeFeed)
        if (bIsTFValid) {
          {
            //--------------------------------------------------
            // trades table
            //--------------------------------------------------
            val prep = _conn.prepareStatement("insert into trades (timestamp,instrument_id,trade_volume,trade_price,buy_sell,order_id,strategy_id) values (?,?,?,?,?,?,?) ")
            prep.setString(1, SUtil.convertTimestampFmt1(csvFields(0))) // timestamp
            prep.setString(2, csvFields(3).toString) // instrument_id
            prep.setDouble(3, csvFields(6).toDouble) // trade_volume
            prep.setDouble(4, csvFields(5).toDouble) // trade_price
            prep.setDouble(5, csvFields(7).toDouble) // buy_sell
            prep.setInt(6, csvFields(9).toInt) // order_id
            prep.setString(7, csvFields(10)) // strategy_id
            prep.executeUpdate
            // _conn.commit
          }
          {
            //--------------------------------------------------
            // signals table
            //--------------------------------------------------
            val ts = SUtil.convertTimestampFmt1(csvFields(0))
            val prep = _conn.prepareStatement("insert into signals (status,timestamp,instrument_id,buy_sell,price,volume,comment,strategy_id,update_timestamp,signal_timestamp) values (?,?,?,?,?,?,?,?,?,?) ")
            prep.setInt(1, 0) // states
            prep.setString(2, ts) //timestamp
            prep.setString(3, csvFields(3).toString) // instrument_id
            prep.setDouble(4, csvFields(7).toDouble) // buy_sell
            prep.setDouble(5, csvFields(5).toDouble) // price
            prep.setDouble(6, csvFields(6).toDouble) // volume
            prep.setString(7, csvFields(4)) // comment
            prep.setString(8, csvFields(10)) // strategy_id
            prep.setString(9, ts) // update_timestamp
            prep.setString(10, ts) // signal_timestamp
            prep.executeUpdate
            _conn.commit
          }
        }
      }
    })
  }

  def batchInsertTradeFeedToDB(ltf: List[String]) {
    lsConn.foreach(_conn => {
      try {

        val prep = _conn.prepareStatement("insert into trades (timestamp,instrument_id,trade_volume,trade_price,buy_sell,order_id,strategy_id) values (?,?,?,?,?,?,?) ")

        ltf.foreach {
          tf =>
            val (bIsTFValid, csvFields) = SUtil.parseAugmentedTradeFeed(tf)
            if (bIsTFValid) {
              prep.setString(1, SUtil.convertTimestampFmt1(csvFields(0)))
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
        _conn.commit

      }
    })
  }

  def insertMarketDataToItrdTbl(lsSymPrice: List[(String, (DateTime, Double))]) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into market_data_intraday (timestamp,instrument_id,nominal_price) values (?,?,?)")

        lsSymPrice.foreach {
          case (symbol, (datetime, nominalprice)) =>
            {
              prep.setString(1, SUtil.convertDateTimeToStr(datetime))
              prep.setString(2, symbol)
              prep.setDouble(3, nominalprice)
              prep.addBatch()
            }
        }

        prep.executeBatch
        _conn.commit
      }
    })
  }

  def insertMarketDataToHourlyTbl(sMarketFeed: String) {
    lsConn.foreach(_conn => {
      try {

        val (bIsMFValid, mfnominal) = SUtil.parseMarketFeedNominal(sMarketFeed)
        if (bIsMFValid) {
          val prep = _conn.prepareStatement("insert into market_data_hourly_hk_stock (timestamp,instrument_id,open,high,low,close,volume) values (?,?,?,?,?,?,?)")

          //--------------------------------------------------
          // TODO correct fake OHLC
          //--------------------------------------------------
          prep.setString(1, SUtil.convertDateTimeToStr(mfnominal.datetime))
          prep.setString(2, mfnominal.symbol)
          prep.setDouble(3, mfnominal.nominal_price)
          prep.setDouble(4, mfnominal.nominal_price)
          prep.setDouble(5, mfnominal.nominal_price)
          prep.setDouble(6, mfnominal.nominal_price)
          prep.setDouble(7, 1)
          prep.executeUpdate
          _conn.commit
        }
      }
    })
  }

  def insertMarketDataToDailyTbl(sMarketFeed: String) {
    lsConn.foreach(_conn => {
      try {

        val (bIsMFValid, mfnominal) = SUtil.parseMarketFeedNominal(sMarketFeed)
        if (bIsMFValid) {
          val prep = _conn.prepareStatement("insert into market_data_daily_hk_stock (timestamp,instrument_id,open,high,low,close,volume) values (?,?,?,?,?,?,?)")

          //--------------------------------------------------
          // TODO correct fake OHLC
          //--------------------------------------------------
          prep.setString(1, SUtil.convertDateTimeToStr(mfnominal.datetime))
          prep.setString(2, mfnominal.symbol)
          prep.setDouble(3, mfnominal.nominal_price)
          prep.setDouble(4, mfnominal.nominal_price)
          prep.setDouble(5, mfnominal.nominal_price)
          prep.setDouble(6, mfnominal.nominal_price)
          prep.setDouble(7, 1)
          prep.executeUpdate
          _conn.commit
        }
      }
    })
  }

  def cleanMarketDataInItrdTbl(dt: DateTime) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("delete from market_data_intraday where timestamp < ?")
        prep.setString(1, SUtil.convertDateTimeToStr(dt))
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def cleanItrdPnLTbl(dt: DateTime) {
    lsConn.foreach(_conn => {
      try {
        val prep1 = _conn.prepareStatement("delete from intraday_pnl where timestamp < ?")
        prep1.setString(1, SUtil.convertDateTimeToStr(dt))
        prep1.executeUpdate
        val prep2 = _conn.prepareStatement("delete from intraday_pnl_per_strategy where timestamp < ?")
        prep2.setString(1, SUtil.convertDateTimeToStr(dt))
        prep2.executeUpdate
        _conn.commit
      }
    })
  }

  def getNominalPricesAsAt(asOfDate: DateTime, symbol: String, mdinmemory: Map[String, (DateTime, Double)], mdfromdb: List[(String, DateTime, Double)]): Map[String, Double] = {

    var symbols = mdinmemory.map { case (symbol, (_, _)) => symbol }.toList
    symbols :::= mdfromdb.map { case (symbol, _, _) => symbol }.toList
    symbols = symbols.distinct

    //--------------------------------------------------
    // combine market date from memory and from database
    //--------------------------------------------------
    val lsTup3 = mdinmemory.map { case (symbol, (datetime, nominal_price)) => (symbol, datetime, nominal_price) }.toList ::: mdfromdb

    //--------------------------------------------------
    // filter with timestamp and group by symbols
    //--------------------------------------------------
    val mapSymTup3 = lsTup3.filter(_._2.getMillis <= asOfDate.getMillis).groupBy(_._1)

    val lsResult = mapSymTup3.map {
      case (symbol, lstup3) => {
        //--------------------------------------------------
        // sort first!
        //--------------------------------------------------
        lstup3.sortWith(_._2.getMillis > _._2.getMillis) match {
          case Nil     => (symbol, new DateTime(), 0.0)
          case x :: xs => x
        }
      }
    }

    var res_map = Map[String, Double]()

    lsResult.foreach(t => res_map += (t._1 -> t._3))

    res_map
  }

  def getNominalPricesFromDBAsAt(asOfDate: DateTime): List[(String, DateTime, Double)] = {
    val _conn = lsConn.head

    var symbols = List[String]()
    var results = List[(String, DateTime, Double)]()

    //--------------------------------------------------
    // just be safe, make sure we can get the latest price, in case the clocks don't perfectly sync
    //--------------------------------------------------
    val asOfDateModified = asOfDate.plusMinutes(15)
    val asOfDateStr = SUtil.convertDateTimeToStr(asOfDateModified)

    //--------------------------------------------------
    // get all symbols first
    //--------------------------------------------------
    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select distinct instrument_id from market_data_daily_hk_stock")
      val rs = prep.executeQuery()

      while (rs.next) {
        symbols ::= rs.getString("instrument_id")
      }
    }

    symbols.foreach { symbol =>
      {
        //--------------------------------------------------
        // from intraday table
        //--------------------------------------------------
        try {
          val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

          val prep = _conn.prepareStatement("select timestamp,instrument_id,nominal_price from market_data_intraday where timestamp <= ? and instrument_id = ? order by timestamp desc limit 2")
          prep.setString(1, asOfDateStr)
          prep.setString(2, symbol)
          val rs = prep.executeQuery()

          while (rs.next) {

            val symbol = rs.getString("instrument_id")
            val datetime = SUtil.convertMySQLTSToDateTime(rs.getString("timestamp"))
            val nominal_price = rs.getDouble("nominal_price")

            results ::= (symbol, datetime, nominal_price)
          }
        }
        // //--------------------------------------------------
        // // from hourly table
        // //--------------------------------------------------
        // try {
        //   val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        //
        //   val prep = _conn.prepareStatement("select timestamp,instrument_id,close from market_data_hourly_hk_stock where timestamp <= ? and instrument_id = ? order by timestamp desc limit 2")
        //   prep.setString(1, asOfDateStr)
        //   prep.setString(2, symbol)
        //   val rs = prep.executeQuery()
        //
        //   while (rs.next) {
        //
        //     val symbol = rs.getString("instrument_id")
        //     val datetime = SUtil.convertMySQLTSToDateTime(rs.getString("timestamp"))
        //     val close = rs.getDouble("close")
        //
        //     results ::= (symbol, datetime, close)
        //   }
        // }
        //--------------------------------------------------
        // from daily table
        //--------------------------------------------------
        try {
          val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

          val prep = _conn.prepareStatement("select timestamp,instrument_id,close from market_data_daily_hk_stock where timestamp <= ? and instrument_id = ? order by timestamp desc limit 2")
          prep.setString(1, asOfDateStr)
          prep.setString(2, symbol)
          val rs = prep.executeQuery()

          while (rs.next) {

            val symbol = rs.getString("instrument_id")
            val datetime = SUtil.convertMySQLTSToDateTime(rs.getString("timestamp"))
            val close = rs.getDouble("close")

            results ::= (symbol, datetime, close)
          }
        }
      }
    }

    results
  }

  def getSymFromPortfolioTbl(strategy_id: String): List[String] = {
    val _conn = lsConn.head

    var results = List[String]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select distinct instrument_id from portfolios where strategy_id=?")
      prep.setString(1, strategy_id)
      val rs = prep.executeQuery()

      while (rs.next) {
        results ::= rs.getString("instrument_id")
      }
    }
    results
  }
  def getSymFromPortfolioTblWithZeroPos(strategy_id: String): List[String] = {
    val _conn = lsConn.head

    var results = List[String]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select distinct instrument_id from portfolios where strategy_id=? and volume=0")
      prep.setString(1, strategy_id)
      val rs = prep.executeQuery()

      while (rs.next) {
        results ::= rs.getString("instrument_id")
      }
    }
    results
  }
  def getSymFromTradeTbl(strategy_id: String): List[String] = {
    val _conn = lsConn.head

    var results = List[String]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select distinct instrument_id from trades where strategy_id=?")
      prep.setString(1, strategy_id)
      val rs = prep.executeQuery()

      while (rs.next) {
        results ::= rs.getString("instrument_id")
      }
    }
    results
  }
  def getTotalPnLOfSty(strategy_id: String): Double = {
    val _conn = lsConn.head

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
    val _conn = lsConn.head

    var results: Option[DateTime] = None

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select timestamp from daily_pnl order by timestamp desc limit 1")

      while (rs.next) {
        results = Some(SUtil.convertMySQLTSToDateTime(rs.getString("timestamp")))
      }
    }
    results
  }

  def getLastPnLOfStySym(strategy_id: String, symbol: String): (Double, Double, Double) = {
    val _conn = lsConn.head

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
    val _conn = lsConn.head

    var results = ListBuffer[String]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select distinct strategy_id from trades order by strategy_id")

      while (rs.next) {
        results += rs.getString("strategy_id")
      }
    }
    results.toList
  }

  def getAllTradesForSty(sty: String): List[TradeFeed] = {
    val _conn = lsConn.head

    var results = ListBuffer[TradeFeed]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select timestamp,instrument_id,trade_price,trade_volume,buy_sell from trades where strategy_id=? order by timestamp")
      prep.setString(1, sty)
      val rs = prep.executeQuery()

      while (rs.next) {
        val datetime = SUtil.convertMySQLTSToDateTime(rs.getString("timestamp"))
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
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into intraday_pnl (timestamp,instrument_id,realized_pnl,unrealized_pnl,total_pnl,position,strategy_id) values (?,?,?,?,?,?,?)")

        prep.setString(1, SUtil.getCurrentSqlTimeStampStr(HongKong()))
        prep.setString(2, symbol)
        prep.setDouble(3, pnlcalcrow.cumRlzdPnL)
        prep.setDouble(4, pnlcalcrow.cumUrlzdPnL)
        prep.setDouble(5, pnlcalcrow.cumRlzdPnL + pnlcalcrow.cumUrlzdPnL)
        prep.setDouble(6, pnlcalcrow.cumSgndVol)
        prep.setString(7, strategy_id)
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def insertPnLCalcRowToItrdPnLTbl(lsStySymPnLRow: List[(String, String, PnLCalcRow)]) {
    val curSqlTime = SUtil.getCurrentSqlTimeStampStr(HongKong())
    lsConn.foreach(_conn => {
      try {

        //--------------------------------------------------
        // with instrument breakdown
        //--------------------------------------------------
        val prep1 = _conn.prepareStatement("insert into intraday_pnl (timestamp,instrument_id,realized_pnl,unrealized_pnl,total_pnl,position,strategy_id) values (?,?,?,?,?,?,?)")

        lsStySymPnLRow.foreach {
          case (strategy_id, symbol, pnlcalcrow) => {

            prep1.setString(1, curSqlTime)
            prep1.setString(2, symbol)
            prep1.setDouble(3, pnlcalcrow.cumRlzdPnL)
            prep1.setDouble(4, pnlcalcrow.cumUrlzdPnL)
            prep1.setDouble(5, pnlcalcrow.cumRlzdPnL + pnlcalcrow.cumUrlzdPnL)
            prep1.setDouble(6, pnlcalcrow.cumSgndVol)
            prep1.setString(7, strategy_id)
            prep1.addBatch
          }
        }
        prep1.executeBatch

        //--------------------------------------------------
        // without instrument breakdown
        //--------------------------------------------------
        val tupstypnlrow =
          lsStySymPnLRow.groupBy(_._1).map {
            case (sty, lstup) => {

              (sty, lstup.map(_._3).fold(PnLCalcRow(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)) {
                (carryOver, e) =>
                  PnLCalcRow(
                    carryOver.trdPx + e.trdPx,
                    carryOver.trdVol + e.trdVol,
                    carryOver.trdSgn + e.trdSgn,
                    carryOver.cumSgndVol + e.cumSgndVol,
                    carryOver.cumSgndNotlAdjChgSgn + e.cumSgndNotlAdjChgSgn,
                    carryOver.avgPx + e.avgPx,
                    carryOver.prdTotPnL + e.prdTotPnL,
                    carryOver.cumTotPnL + e.cumTotPnL,
                    carryOver.psnClosed + e.psnClosed,
                    carryOver.rlzdPnL + e.rlzdPnL,
                    carryOver.cumRlzdPnL + e.cumRlzdPnL,
                    carryOver.cumUrlzdPnL + e.cumUrlzdPnL
                  )
              })

            }
          }

        val prep2 = _conn.prepareStatement("insert into intraday_pnl_per_strategy (timestamp,realized_pnl,unrealized_pnl,total_pnl,strategy_id) values (?,?,?,?,?)")

        tupstypnlrow.foreach {
          case (strategy_id, pnlcalcrow) =>
            {
              prep2.setString(1, curSqlTime)
              prep2.setDouble(2, pnlcalcrow.cumRlzdPnL)
              prep2.setDouble(3, pnlcalcrow.cumUrlzdPnL)
              prep2.setDouble(4, pnlcalcrow.cumRlzdPnL + pnlcalcrow.cumUrlzdPnL)
              prep2.setString(5, strategy_id)
              prep2.addBatch
            }
        }

        prep2.executeBatch

        _conn.commit
      }
    })
  }

  def insertPnLCalcRowToDailyPnLTbl(dt: Option[DateTime], lsStySymPnLRow: List[(String, String, PnLCalcRow)]) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into daily_pnl (timestamp,instrument_id,realized_pnl,unrealized_pnl,position,strategy_id) values (?,?,?,?,?,?)")

        val dtToUse = dt match {
          case Some(dt: DateTime) => dt
          case _                  => SUtil.getCurrentDateTime(HongKong())
        }

        lsStySymPnLRow.foreach {
          case (strategy_id, symbol, pnlcalcrow) => {
            prep.setString(1, SUtil.convertDateTimeToStr(dtToUse))
            prep.setString(2, symbol)
            prep.setDouble(3, pnlcalcrow.cumRlzdPnL)
            prep.setDouble(4, pnlcalcrow.cumUrlzdPnL)
            prep.setDouble(5, pnlcalcrow.cumSgndVol)
            prep.setString(6, strategy_id)
            prep.addBatch
          }
        }

        prep.executeBatch
        _conn.commit
      }
    })
  }

  def insertPnLCalcRowToDailyPnLTbl(dt: Option[DateTime], strategy_id: String, symbol: String, pnlcalcrow: PnLCalcRow) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into daily_pnl (timestamp,instrument_id,realized_pnl,unrealized_pnl,position,strategy_id) values (?,?,?,?,?,?)")

        val dtToUse = dt match {
          case Some(dt: DateTime) => dt
          case _                  => SUtil.getCurrentDateTime(HongKong())
        }

        prep.setString(1, SUtil.convertDateTimeToStr(dtToUse))
        prep.setString(2, symbol)
        prep.setDouble(3, pnlcalcrow.cumRlzdPnL)
        prep.setDouble(4, pnlcalcrow.cumUrlzdPnL)
        prep.setDouble(5, pnlcalcrow.cumSgndVol)
        prep.setString(6, strategy_id)
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def insertPortfolioTbl(dt: Option[DateTime], lsStySymPnLRow: List[(String, String, PnLCalcRow)]) {
    lsConn.foreach(_conn => {
      try {
        val dtToUse = dt match {
          case Some(dt: DateTime) => dt
          case _                  => SUtil.getCurrentDateTime(HongKong())
        }

        val prep = _conn.prepareStatement("insert into portfolios (instrument_id,volume,avg_price,timestamp,strategy_id,unrealized_pnl) values (?,?,?,?,?,?)")

        lsStySymPnLRow.foreach {
          case (strategy_id, symbol, pnlcalcrow) => {
            prep.setString(1, symbol)
            prep.setDouble(2, pnlcalcrow.cumSgndVol)
            prep.setDouble(3, pnlcalcrow.avgPx)
            prep.setString(4, SUtil.convertDateTimeToStr(dtToUse))
            prep.setString(5, strategy_id)
            prep.setDouble(6, pnlcalcrow.cumUrlzdPnL)
            prep.addBatch
          }
        }

        prep.executeBatch
        _conn.commit
      }
    })
  }

  def removeSymFromPortfolioTbl() {
    lsConn.foreach(_conn => {
      try {
        //--------------------------------------------------
        // remove symbols that have no tradefeed
        //--------------------------------------------------
        val allSty = DBProcessor.getAllStyFromTradesTable

        allSty.foreach(strategy_id => {
          val lsSymInPortfolios = getSymFromPortfolioTbl(strategy_id)
          val lsSymInTrades = getSymFromTradeTbl(strategy_id)
          val lsUnwantedSym = lsSymInPortfolios.filter(!lsSymInTrades.contains(_))

          lsUnwantedSym.foreach(deletePortfolioTableWithStratIDSym(strategy_id, _))
        })

        //--------------------------------------------------
        // remove symbols that have zero position
        //--------------------------------------------------
        allSty.foreach(strategy_id => {
          val lsUnwantedSym = DBProcessor.getSymFromPortfolioTblWithZeroPos(strategy_id)
          lsUnwantedSym.foreach(deletePortfolioTableWithStratIDSym(strategy_id, _))
        })

      }
    })
  }

  def updateOrInsertPortfolioTbl(dt: Option[DateTime], lsStySymPnLRow: List[(String, String, PnLCalcRow)], mdinmemory: Map[String, (DateTime, Double)], mdfromdb: List[(String, DateTime, Double)]) {
    lsConn.foreach(_conn => {
      try {

        val dtToUse = dt match {
          case Some(dt: DateTime) => dt
          case _                  => SUtil.getCurrentDateTime(HongKong())
        }

        val prep1 = _conn.prepareStatement("select count(*) as cnt from portfolios where strategy_id=? and instrument_id=?")
        val prep2 = _conn.prepareStatement("insert into portfolios (instrument_id,volume,avg_price,timestamp,strategy_id,unrealized_pnl,market_value) values (?,?,?,?,?,?,?)")
        val prep3 = _conn.prepareStatement("update portfolios set volume=?,avg_price=?,timestamp=?,unrealized_pnl=?,market_value=? where strategy_id=? and instrument_id=?")

        lsStySymPnLRow.foreach {
          case (strategy_id, symbol, pnlcalcrow) => {

            val price = mdinmemory.get(symbol) match {
              case None => {
                mdfromdb.filter(_._1 == symbol).lift(0) match {
                  case None            => 0.0
                  case Some((_, _, p)) => p
                  case _               => 0.0
                }
              }
              case Some((_, p)) => p
              case _            => 0.0
            }

            prep1.setString(1, strategy_id)
            prep1.setString(2, symbol)

            val rs = prep1.executeQuery

            val cnt = if (rs.next) rs.getInt("cnt") else 0

            if (cnt == 0) {
              //--------------------------------------------------
              // insert
              //--------------------------------------------------
              prep2.setString(1, symbol)
              prep2.setDouble(2, pnlcalcrow.cumSgndVol)
              prep2.setDouble(3, pnlcalcrow.avgPx)
              prep2.setString(4, SUtil.convertDateTimeToStr(dtToUse))
              prep2.setString(5, strategy_id)
              prep2.setDouble(6, pnlcalcrow.cumUrlzdPnL)
              prep2.setDouble(7, pnlcalcrow.cumSgndVol * price)
              prep2.addBatch
            }
            else {
              //--------------------------------------------------
              // update
              //--------------------------------------------------
              prep3.setDouble(1, pnlcalcrow.cumSgndVol)
              prep3.setDouble(2, pnlcalcrow.avgPx)
              prep3.setString(3, SUtil.convertDateTimeToStr(dtToUse))
              prep3.setDouble(4, pnlcalcrow.cumUrlzdPnL)
              prep3.setDouble(5, pnlcalcrow.cumSgndVol * price)

              prep3.setString(6, strategy_id)
              prep3.setString(7, symbol)
              prep3.addBatch
            }

          }
        }

        prep2.executeBatch
        prep3.executeBatch
        _conn.commit
      }
    })
  }

  def insertPortfolioTbl(dt: Option[DateTime], strategy_id: String, symbol: String, signedPos: Double, avgPx: Double, cumUrlzdPnL: Double) {
    lsConn.foreach(_conn => {
      try {
        val prep = _conn.prepareStatement("insert into portfolios (instrument_id,volume,avg_price,timestamp,strategy_id,unrealized_pnl) values (?,?,?,?,?,?)")

        val dtToUse = dt match {
          case Some(dt: DateTime) => dt
          case _                  => SUtil.getCurrentDateTime(HongKong())
        }
        prep.setString(1, symbol)
        prep.setDouble(2, signedPos)
        prep.setDouble(3, avgPx)
        prep.setString(4, SUtil.convertDateTimeToStr(dtToUse))
        prep.setString(5, strategy_id)
        prep.setDouble(6, cumUrlzdPnL)
        prep.executeUpdate
        _conn.commit
      }
    })
  }

  def getAllTradesOfSty(sid: String): List[TradeFeed] = {
    val _conn = lsConn.head

    var results = ListBuffer[TradeFeed]()

    try {
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val prep = _conn.prepareStatement("select timestamp,instrument_id,trade_price,trade_volume,buy_sell from trades where strategy_id=?")
      prep.setString(1, sid)
      val rs = prep.executeQuery

      while (rs.next) {
        val datetime = SUtil.convertMySQLTSToDateTime(rs.getString("timestamp"))
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

  def updateOrInsertTradingAccountTbl(mapStyRlzdPnL: Map[String, Double]) {

    //--------------------------------------------------
    // calculate available cash
    //--------------------------------------------------
    val allSty = DBProcessor.getAllStyFromTradesTable.filter(Config.initCapital.contains(_))

    val lsTupStyCF = {
      allSty.map(s => (s,
        {
          val lsCF = (DBProcessor.getAllTradesForSty(s)).map(tf => tf.trade_price * tf.trade_volume * { if (tf.trade_sign == 1) 1.0 else -1.0 })
          lsCF.fold(0.0) { (carry, e) => carry + e }
        }))
    }

    //--------------------------------------------------
    lsTupStyCF.foreach {
      case (strategy_id, cashflow) => {
        lsConn.foreach(_conn => {
          try {

            val dtSqlStr = SUtil.convertDateTimeToStr(SUtil.getCurrentDateTime(HongKong()))

            val prep1 = _conn.prepareStatement("select count(*) as cnt from trading_account where strategy_id=?")
            val prep2 = _conn.prepareStatement("insert into trading_account (cash,avail_cash,holding_cash,timestamp,strategy_id) values (?,?,?,?,?)")
            val prep3 = _conn.prepareStatement("update trading_account set cash=?,avail_cash=?,holding_cash=?,timestamp=? where strategy_id=?")

            prep1.setString(1, strategy_id)

            val rs = prep1.executeQuery

            val cnt = if (rs.next) rs.getInt("cnt") else 0

            val styRlzdPnL = mapStyRlzdPnL.get(strategy_id).getOrElse(0.0)
            if (cnt == 0) {
              //--------------------------------------------------
              // insert
              //--------------------------------------------------
              prep2.setDouble(1, Config.initCapital.get(strategy_id).getOrElse(0.0) - cashflow) // cash
              prep2.setDouble(2, Config.initCapital.get(strategy_id).getOrElse(0.0) - cashflow) // avail_cash
              prep2.setDouble(3, 0) // holding_cash
              prep2.setString(4, dtSqlStr)
              prep2.setString(5, strategy_id)
              prep2.executeUpdate
              _conn.commit
            }
            else {
              //--------------------------------------------------
              // update
              //--------------------------------------------------
              prep3.setDouble(1, Config.initCapital.get(strategy_id).getOrElse(0.0) - cashflow) // cash
              prep3.setDouble(2, Config.initCapital.get(strategy_id).getOrElse(0.0) - cashflow) // avail_cash
              prep3.setDouble(3, 0) // holding_cash
              prep3.setString(4, dtSqlStr)
              prep3.setString(5, strategy_id)
              prep3.executeUpdate
              _conn.commit
            }

          }
        })
      }
    }
  }

  def closeConn(): Unit = {
    lsConn.foreach(_.close)
  }

}
