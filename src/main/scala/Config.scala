import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;
import org.joda.time.{DateTime, LocalDate, LocalTime}

object Config {

  def readPropFile(propFileName: String) {

    try {
      val prop = new Properties()
      prop.load(new FileInputStream(propFileName))

      (1 to 9).foreach(i => {
        val connStr = prop.getProperty("jdbcConnStr" + i)
        if (connStr != "" && connStr != null) {
          jdbcConnStr ::= connStr
          jdbcUser ::= prop.getProperty("jdbcUser" + i)
          jdbcPwd ::= prop.getProperty("jdbcPwd" + i)
        }
      })

      zmqMDConnStr = prop.getProperty("zmqMDConnStr")
      zmqTFConnStr = prop.getProperty("zmqTFConnStr")
      pnlCalcIntvlInSec = Option(prop.getProperty("pnlCalcIntvlInSec")) match {
        case Some(s: String) => s.toInt
        case None         => 300
      }
      itrdMktDataUpdateIntvlInSec = Option(prop.getProperty("itrdMktDataUpdateIntvlInSec")) match {
        case Some(s: String) => s.toInt
        case None         => 300
      }
      timeForDailyPnLCalnHHMM = Option(prop.getProperty("timeForDailyPnLCalnHHMM")) match {
        case Some(s: String) => s.toInt
        case None         => 1615
      }

      mtmTime = new LocalTime(timeForDailyPnLCalnHHMM / 100, timeForDailyPnLCalnHHMM % 100)

      onlyCalcPnLDuringTradingHr = prop.getProperty("onlyCalcPnLDuringTradingHr").toBoolean
      doPriceInitialization = prop.getProperty("doPriceInitialization").toBoolean

      //--------------------------------------------------
      styIDToMaintainTradingAccount = prop.getProperty("styIDToMaintainTradingAccount").split(",").toList.map(_.toInt)
      styIDToMaintainTradingAccount.foreach(s => {
        initCapital += s -> prop.getProperty("initCapital_" + s).toDouble
      })
      //--------------------------------------------------

      println(jdbcConnStr)
      println(jdbcUser)
      println(jdbcPwd)
      println(zmqMDConnStr)
      println(zmqTFConnStr)
      println(pnlCalcIntvlInSec)
      println(itrdMktDataUpdateIntvlInSec)
      println(timeForDailyPnLCalnHHMM)
      println(onlyCalcPnLDuringTradingHr)

    }
    catch {
      case e: Exception =>
        {
          e.printStackTrace()
          sys.exit(1)
        }
    }
  }

  //--------------------------------------------------
  // PnL
  //--------------------------------------------------
  var pnlCalcIntvlInSec = 300
  var itrdMktDataUpdateIntvlInSec = 300
  var timeForDailyPnLCalnHHMM = 1615
  var mtmTime: LocalTime = new LocalTime(0, 0)
  var ldStartCalcPnL: LocalDate = new LocalDate(2016, 1, 1)
  var onlyCalcPnLDuringTradingHr: Boolean = true

  //--------------------------------------------------
  // initial capital
  //--------------------------------------------------
  var initCapital = Map[Int, Double]()
  var styIDToMaintainTradingAccount = List[Int]()

  //--------------------------------------------------
  // JDBC
  //--------------------------------------------------
  var jdbcConnStr = List[String]()
  var jdbcUser = List[String]()
  var jdbcPwd = List[String]()

  //--------------------------------------------------
  // zmq
  //--------------------------------------------------
  var zmqMDConnStr = ""
  var zmqTFConnStr = ""

  //--------------------------------------------------
  // persist MD to DB
  //--------------------------------------------------
  var MDPersistIntervalInSec = 300
  var CongestionThresholdInSec = 10

  //--------------------------------------------------
  // print market data for stock
  //--------------------------------------------------
  val symbolToPrintMD = "00941"

  //--------------------------------------------------
  var doPriceInitialization: Boolean = true

}
