import java.sql.{Connection, DriverManager, ResultSet, Timestamp};
import java.util.Properties;


object DBProcessor
{
  //--------------------------------------------------
  // mysql
  //--------------------------------------------------

    Class.forName("com.mysql.jdbc.Driver")

    var p = new Properties()
    p.put("user",Config.jdbcUser)
    p.put("password",Config.jdbcPwd)
    val _conn = DriverManager.getConnection(Config.jdbcConnStr,p)


  def deleteTradeTable()
  {
    try {
      val prep = _conn.prepareStatement("delete from trades")
        prep.executeUpdate
    }
  }
  def parseTradeFeed(sTradeFeed: String): (Boolean, List[String]) =
  {
    val csvFields = sTradeFeed.split(",")
      if (csvFields.length == 10) (true,csvFields.toList)
      else (false,List())
  }

  def updateTradeFeedToDB(sTradeFeed: String)
  {
    try {

      val (bIsTFValid, csvFields) = parseTradeFeed(sTradeFeed)
        if (bIsTFValid)
        {
          val prep = _conn.prepareStatement("insert into trades (timestamp,instrument_id,trade_volume,trade_price,buy_sell,order_id) values (?,?,?,?,?,?) ")

            prep.setString(1, Util.convertTimestampFmt1(csvFields(0)))
            prep.setString(2, csvFields(3).toString)
            prep.setFloat(3, csvFields(6).toFloat)
            prep.setFloat(4, csvFields(5).toFloat)
            prep.setFloat(5, csvFields(7).toFloat)
            prep.setInt(6, csvFields(9).toInt)
            prep.executeUpdate
        }

    }
  }
  def viewTable(s: String)
  {

    try {
      // Configure to be Read Only
      val statement = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

        // Execute Query
        val rs = statement.executeQuery("select * from testtable")

        // Iterate Over ResultSet
        while (rs.next) {
          println(rs.getString("first"))
        }
    }

  }

  def closeConn() : Unit = {
    _conn.close
  }
}
