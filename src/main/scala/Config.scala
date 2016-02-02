import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;
import org.joda.time.{DateTime, LocalTime}

object Config {

  def readPropFile(propFileName: String) {

    try {

      val prop = new Properties()
      prop.load(new FileInputStream(propFileName))

      jdbcConnStr = prop.getProperty("jdbcConnStr")
      jdbcUser = prop.getProperty("jdbcUser")
      jdbcPwd = prop.getProperty("jdbcPwd")
      zmqMDConnStr = prop.getProperty("zmqMDConnStr")
      zmqTFConnStr = prop.getProperty("zmqTFConnStr")
      pnlCalcIntvlInSec = Option(prop.getProperty("pnlCalcIntvlInSec")) match {
        case Some(s: String) => s.toInt
        case None            => 300
      }
      itrdMktDataUpdateIntvlInSec = Option(prop.getProperty("itrdMktDataUpdateIntvlInSec")) match {
        case Some(s: String) => s.toInt
        case None            => 300
      }

      println(jdbcConnStr)
      println(jdbcUser)
      println(jdbcPwd)
      println(zmqMDConnStr)
      println(zmqTFConnStr)
      println(pnlCalcIntvlInSec)
      println(itrdMktDataUpdateIntvlInSec)

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
  var mtmTime: LocalTime = new LocalTime(16, 15)
  var dtStartCalcPnL: DateTime = new DateTime(2016, 1, 1, 17, 0, 0)

  //--------------------------------------------------
  // JDBC
  //--------------------------------------------------
  var jdbcConnStr = ""
  var jdbcUser = ""
  var jdbcPwd = ""

  //--------------------------------------------------
  // zmq
  //--------------------------------------------------
  var zmqMDConnStr = ""
  var zmqTFConnStr = ""

}
