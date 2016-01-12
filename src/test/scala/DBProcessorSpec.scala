import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before

class DBProcessorTest extends AssertionsForJUnit {

  @Before def initialize() {
  }

  @Test def testUpdateTradeFeedToDB() {
      DBProcessor.deleteTradeTable
      DBProcessor.updateTradeFeedToDB("20151216_045514_142013,tradefeed,HKIF,GDXJ,OID_20151217_045507__2,19.84,62,2,31,77")
      assert(true)
  }
}
