import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.joda.time.{Period, DateTime, Duration}

class DBProcessorTest extends AssertionsForJUnit {

  @Before def initialize() {
  }

  @Test def testPutAndGetMarketData2() {
    Config.readPropFile("/home/qy/Dropbox/nirvana/sbtProj/stratEngine/132.properties")
    DBProcessor.deleteMDItrdTable
    DBProcessor.insertMarketDataToHourlyTbl("20151211_155514_142013,941,90")
    DBProcessor.insertMarketDataToHourlyTbl("20151212_155514_142013,941,91")
    DBProcessor.insertMarketDataToHourlyTbl("20151213_155514_142013,941,92")
    DBProcessor.insertMarketDataToHourlyTbl("20151214_155514_142013,941,93")
    DBProcessor.insertMarketDataToItrdTbl("20151215_155514_142013,941,94")
    DBProcessor.insertMarketDataToItrdTbl("20151216_155514_142013,941,95")
    DBProcessor.insertMarketDataToItrdTbl("20151217_155514_142013,941,96")
    DBProcessor.insertMarketDataToItrdTbl("20151218_155514_142013,941,97")
    DBProcessor.insertMarketDataToHourlyTbl("20151219_155514_142013,941,98")
    DBProcessor.insertMarketDataToHourlyTbl("20151220_155514_142013,941,99")
    DBProcessor.insertMarketDataToHourlyTbl("20151221_155514_142013,941,100")
    DBProcessor.insertMarketDataToDailyTbl("20151222_155514_142013,941,101")
    DBProcessor.insertMarketDataToDailyTbl("20151223_155514_142013,941,102")
    DBProcessor.insertMarketDataToDailyTbl("20151224_155514_142013,941,103")
    DBProcessor.insertMarketDataToItrdTbl("20151225_155514_142013,941,104")
    DBProcessor.insertMarketDataToItrdTbl("20151226_155514_142013,941,105")
    DBProcessor.insertMarketDataToItrdTbl("20151227_155514_142013,941,106")
    DBProcessor.insertMarketDataToItrdTbl("20151228_155514_142013,941,107")
    DBProcessor.insertMarketDataToDailyTbl("20151229_155514_142013,941,108")
    DBProcessor.insertMarketDataToDailyTbl("20151230_155514_142013,941,109")
    DBProcessor.insertMarketDataToDailyTbl("20151231_155514_142013,941,110")
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 11, 23, 59, 59)).get("941").getOrElse(0), 90.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 12, 23, 59, 59)).get("941").getOrElse(0), 91.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 13, 23, 59, 59)).get("941").getOrElse(0), 92.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 14, 23, 59, 59)).get("941").getOrElse(0), 93.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 15, 23, 59, 59)).get("941").getOrElse(0), 94.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 16, 23, 59, 59)).get("941").getOrElse(0), 95.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 17, 23, 59, 59)).get("941").getOrElse(0), 96.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 18, 23, 59, 59)).get("941").getOrElse(0), 97.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 19, 23, 59, 59)).get("941").getOrElse(0), 98.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 20, 23, 59, 59)).get("941").getOrElse(0), 99.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 21, 23, 59, 59)).get("941").getOrElse(0), 100.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 22, 23, 59, 59)).get("941").getOrElse(0), 101.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 23, 23, 59, 59)).get("941").getOrElse(0), 102.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 24, 23, 59, 59)).get("941").getOrElse(0), 103.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 25, 23, 59, 59)).get("941").getOrElse(0), 104.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 26, 23, 59, 59)).get("941").getOrElse(0), 105.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 27, 23, 59, 59)).get("941").getOrElse(0), 106.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 28, 23, 59, 59)).get("941").getOrElse(0), 107.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 29, 23, 59, 59)).get("941").getOrElse(0), 108.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 30, 23, 59, 59)).get("941").getOrElse(0), 109.0)
    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 31, 23, 59, 59)).get("941").getOrElse(0), 110.0)

    assertEquals(DBProcessor.getNominalPricesAsAt(new DateTime(2015, 12, 31, 23, 59, 59)).get("2628").getOrElse(0), 0)
  }

  @Test def testScen1() {
    //--------------------------------------------------
    // read config
    //--------------------------------------------------
    Config.readPropFile("/home/qy/Dropbox/nirvana/sbtProj/stratEngine/132.properties")
    //--------------------------------------------------
    // delete tables
    //--------------------------------------------------
    DBProcessor.deleteMDItrdTable
    DBProcessor.deleteSignalsTable
    DBProcessor.deleteTradesTable
    DBProcessor.deleteMDItrdTable
    DBProcessor.deleteItrdPnLTable
    DBProcessor.deletePortfolioTable
    //--------------------------------------------------
    // insertTradeFeedToDB
    //--------------------------------------------------
    DBProcessor.insertTradeFeedToDB("20160115_103431_000000,tradefeed,IEX,ANAC,OID,93.770000,12,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_103753_000000,tradefeed,IBKRATS,ELLI,OID,61.890000,20,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_103804_000000,tradefeed,IBKRATS,BSTC,OID,38.350000,27,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_104002_000000,tradefeed,IEX,CPB,OID,52.730000,23,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_104028_000000,tradefeed,ARCA,ALDW,OID,20.900000,55,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_104456_000000,tradefeed,ISLAND,CHDN,OID,139.020000,9,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_104819_000000,tradefeed,IBKRATS,AMSG,OID,67.310000,16,2,TID,0,KANGAROO1")
    DBProcessor.insertTradeFeedToDB("20160115_104831_000000,tradefeed,IBKRATS,COT,OID,9.941000,117,2,TID,0,KANGAROO2")
    DBProcessor.insertTradeFeedToDB("20160115_111410_000000,tradefeed,IBKRATS,ADUS,OID,20.437000,52,2,TID,0,KANGAROO2")
    DBProcessor.insertTradeFeedToDB("20160115_111530_000000,tradefeed,DARK,BFAM,OID,62.090000,19,2,TID,0,KANGAROO2")
    DBProcessor.insertTradeFeedToDB("20160115_112749_000000,tradefeed,DARK,CENTA,OID,12.141100,86,2,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160115_113600_000000,tradefeed,ARCA,CENT,OID,12.360000,89,2,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160115_114714_000000,tradefeed,IBKRATS,AFAM,OID,37.069000,31,2,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160115_124022_000000,tradefeed,BYX,CIVI,OID,24.780000,42,2,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160115_142117_000000,tradefeed,BEX,EIRL,OID,38.230000,30,2,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_095941_000000,tradefeed,IBKRATS,ELNK,OID,5.741000,176,2,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103425_000000,tradefeed,DARK,CMC,OID,13.018900,96,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103501_000000,tradefeed,IBKRATS,BRSS,OID,19.385000,64,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103610_000000,tradefeed,DARK,BANR,OID,41.940000,29,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103643_000000,tradefeed,ISLAND,COMM,OID,22.660000,55,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103646_000000,tradefeed,ARCA,AAOI,OID,13.460000,92,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103735_000000,tradefeed,DARK,CCU,OID,19.228900,64,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103751_000000,tradefeed,DRCTEDGE,CBF,OID,29.120000,42,1,TID,0,KANGAROO3")
    DBProcessor.insertTradeFeedToDB("20160119_103823_000000,tradefeed,DARK,CVRR,OID,17.149900,72,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103831_000000,tradefeed,DARK,CVI,OID,34.668900,35,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103836_000000,tradefeed,IBKRATS,ALJ,OID,12.099700,103,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103921_000000,tradefeed,DARK,ALJ,OID,17.869500,69,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103940_000000,tradefeed,DARK,ALJ,OID,34.540000,3,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103943_000000,tradefeed,BYX,ALJ,OID,71.490000,17,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104017_000000,tradefeed,DARK,ALJ,OID,5.788900,200,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104018_000000,tradefeed,IBKRATS,ALJ,OID,9.5,15,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104019_000000,tradefeed,IBKRATS,ALJ,OID,7,60,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104020_000000,tradefeed,IBKRATS,ALJ,OID,4,91,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104021_000000,tradefeed,IBKRATS,ALJ,OID,10,98,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104022_000000,tradefeed,IBKRATS,ALJ,OID,8,400,1,TID,0,KANGAROO4")

    //--------------------------------------------------
    DBProcessor.insertTradeFeedToDB("20160119_104023_000000,tradefeed,IBKRATS,ALJ,OID,7,16,1,TID,0,KANGAROO4")
    val dt1 = new DateTime(2016, 1, 19, 10, 40, 23)
    StrategyEngine.calcMtmPnL(dt1)
      .foreach {
        case (x, y, z) =>
          DBProcessor.insertPnLCalcRowToItrdPnLTbl(x, y, z)
          DBProcessor.insertPortfolioTbl(Some(dt1), x, y, z.cumSgndVol, z.avgPx)
      }

    val (r1, u1, t1) = DBProcessor.getLastPnLOfStySym("KANGAROO4", "ALJ")
    assertEquals(r1, -2750.69, Util.SMALLNUM)
    assertEquals(u1, -122D, Util.SMALLNUM)
    assertEquals(t1, -2872.69, Util.SMALLNUM)

    DBProcessor.insertTradeFeedToDB("20160119_104024_000000,tradefeed,IBKRATS,ALJ,OID,5,138,2,TID,0,KANGAROO4")
    val dt2 = new DateTime(2016, 1, 19, 10, 40, 24)
    StrategyEngine.calcMtmPnL(dt2)
      .foreach {
        case (x, y, z) =>
          DBProcessor.insertPnLCalcRowToItrdPnLTbl(x, y, z)
          DBProcessor.insertPortfolioTbl(Some(dt2), x, y, z.cumSgndVol, z.avgPx)
      }
    val (r2, u2, t2) = DBProcessor.getLastPnLOfStySym("KANGAROO4", "ALJ")
    assertEquals(r2, -3148.69, Util.SMALLNUM)
    assertEquals(u2, 0D, Util.SMALLNUM)
    assertEquals(t2, -3148.69, Util.SMALLNUM)

    //--------------------------------------------------
    // getAllStyFromTradesTable
    //--------------------------------------------------
    val allStyId = DBProcessor.getAllStyFromTradesTable
    (1 to 4).foreach { i => assert(allStyId(i - 1) == "KANGAROO" + i.toString) }

    //--------------------------------------------------
    // getAllTradesForSty
    //--------------------------------------------------
    val lsTF1 = DBProcessor.getAllTradesForSty("KANGAROO1")
    assertEquals(lsTF1.length, 7)
    assertEquals(lsTF1(0).symbol, "ANAC")
    assertEquals(lsTF1(1).symbol, "ELLI")
    assertEquals(lsTF1(2).trade_price, 38.35, Util.EPSILON)
    assertEquals(lsTF1(3).trade_volume, 23, Util.EPSILON)
    assertEquals(lsTF1(4).trade_sign, -1)

    val lsTF2 = DBProcessor.getAllTradesForSty("KANGAROO2")
    assertEquals(lsTF2.length, 3)

    val lsTF4 = DBProcessor.getAllTradesForSty("KANGAROO4")
    assertEquals(lsTF4.length, 14)

  }

  @Test def testScen2() {
    //--------------------------------------------------
    // read config
    //--------------------------------------------------
    Config.readPropFile("/home/qy/Dropbox/nirvana/sbtProj/stratEngine/132.properties")
    //--------------------------------------------------
    // delete tables
    //--------------------------------------------------
    DBProcessor.deleteMDItrdTable
    DBProcessor.deleteSignalsTable
    DBProcessor.deleteTradesTable
    DBProcessor.deleteMDItrdTable
    DBProcessor.deleteItrdPnLTable
    DBProcessor.deletePortfolioTable
    //--------------------------------------------------
    // insertTradeFeedToDB
    //--------------------------------------------------
    DBProcessor.insertTradeFeedToDB("20160119_103836_000000,tradefeed,IBKRATS,ALJ,OID,12.099700,103,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103921_000000,tradefeed,DARK,ALJ,OID,17.869500,69,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103940_000000,tradefeed,DARK,ALJ,OID,34.540000,172,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_103943_000000,tradefeed,BYX,ALJ,OID,71.490000,17,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104017_000000,tradefeed,DARK,ALJ,OID,5.788900,200,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104018_000000,tradefeed,IBKRATS,ALJ,OID,9.5,15,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104019_000000,tradefeed,IBKRATS,ALJ,OID,7,198,1,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104020_000000,tradefeed,IBKRATS,ALJ,OID,4,91,2,TID,0,KANGAROO4")

    //--------------------------------------------------
    DBProcessor.insertTradeFeedToDB("20160119_104021_000000,tradefeed,IBKRATS,ALJ,OID,10,98,2,TID,0,KANGAROO4")
    val dt1 = new DateTime(2016, 1, 19, 10, 40, 21)
    StrategyEngine.calcMtmPnL(dt1)
      .foreach {
        case (x, y, z) =>
          DBProcessor.insertPnLCalcRowToItrdPnLTbl(x, y, z)
          DBProcessor.insertPortfolioTbl(Some(dt1), x, y, z.cumSgndVol, z.avgPx)
      }
    val (r0, u0, t0) = DBProcessor.getLastPnLOfStySym("KANGAROO4", "ALJ")
    assertEquals(r0, 2160.565, Util.SMALLNUM)
    assertEquals(u0, -546.0, Util.SMALLNUM)
    assertEquals(t0, 1614.565, Util.SMALLNUM)

    DBProcessor.insertTradeFeedToDB("20160119_104022_000000,tradefeed,IBKRATS,ALJ,OID,8,400,1,TID,0,KANGAROO4")
    val dt2 = new DateTime(2016, 1, 19, 10, 40, 22)
    StrategyEngine.calcMtmPnL(dt2)
      .foreach {
        case (x, y, z) =>
          DBProcessor.insertPnLCalcRowToItrdPnLTbl(x, y, z)
          DBProcessor.insertPortfolioTbl(Some(dt2), x, y, z.cumSgndVol, z.avgPx)
      }
    val (r1, u1, t1) = DBProcessor.getLastPnLOfStySym("KANGAROO4", "ALJ")
    assertEquals(r1, 1992.565, Util.SMALLNUM)
    assertEquals(u1, 0.0, Util.SMALLNUM)
    assertEquals(t1, 1992.565, Util.SMALLNUM)

    DBProcessor.insertTradeFeedToDB("20160119_104023_000000,tradefeed,IBKRATS,ALJ,OID,7,211,2,TID,0,KANGAROO4")
    DBProcessor.insertTradeFeedToDB("20160119_104024_000000,tradefeed,IBKRATS,ALJ,OID,5,138,2,TID,0,KANGAROO4")
    val dt3 = new DateTime(2016, 1, 19, 10, 40, 24)
    StrategyEngine.calcMtmPnL(dt3)
      .foreach {
        case (x, y, z) =>
          DBProcessor.insertPnLCalcRowToItrdPnLTbl(x, y, z)
          DBProcessor.insertPortfolioTbl(Some(dt3), x, y, z.cumSgndVol, z.avgPx)
      }
    val (r2, u2, t2) = DBProcessor.getLastPnLOfStySym("KANGAROO4", "ALJ")
    assertEquals(r2, 1781.565, Util.SMALLNUM)
    assertEquals(u2, 0.0, Util.SMALLNUM)
    assertEquals(t2, 1781.565, Util.SMALLNUM)
    //--------------------------------------------------

  }

  // @Test def testCalcPnL() {
  //   //--------------------------------------------------
  //   // read config
  //   //--------------------------------------------------
  //   Config.readPropFile("/home/qy/Dropbox/nirvana/sbtProj/stratEngine/132.properties")
  //
  //   //--------------------------------------------------
  //   // delete tables
  //   //--------------------------------------------------
  //   DBProcessor.deleteMDItrdTable
  //   DBProcessor.deleteSignalsTable
  //   DBProcessor.deleteTradesTable
  //   DBProcessor.deleteMDItrdTable
  //   DBProcessor.deleteItrdPnLTable
  //   DBProcessor.deletePortfolioTable
  //
  //   //--------------------------------------------------
  //   // marketfeed and tradefeed
  //   //--------------------------------------------------
  //   val mfs = scala.io.Source.fromFile("/home/qy/Dropbox/nirvana/sbtProj/stratEngine/forUnitTesting/mf.csv").getLines.toList
  //   mfs.foreach(l => DBProcessor.insertMarketDataToItrdTbl(l.toString))
  //
  //   val tfs = scala.io.Source.fromFile("/home/qy/Dropbox/nirvana/sbtProj/stratEngine/forUnitTesting/tf.csv").getLines.toList
  //   DBProcessor.batchInsertTradeFeedToDB(tfs)
  //   StrategyEngine.calcMtmPnL(new DateTime(2013, 12, 31, 23, 59, 59))
  //
  // }

}
