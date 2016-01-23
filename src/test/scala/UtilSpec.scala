import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before

class UtilTest extends AssertionsForJUnit {

  @Before def initialize() {
  }

  @Test def testConvertTimestamp() {
    assert(Util.convertTimestampFmt1("20151216_045514_142013") == "2015-12-16 04:55:14.142013")
    assert(Util.convertTimestampFmt1("20151216_145514_000000") == "2015-12-16 14:55:14.000000")
  }

  @Test def testIsFloat() {
    assert(Util.isDouble("1.4"))
    assert(Util.isDouble("14"))
    assert(Util.isDouble("-1"))
    assert(Util.isDouble("-3.14"))
    assertFalse(Util.isDouble("1.4a"))
    assertFalse(Util.isDouble("abc"))
    assertFalse(Util.isDouble("a1.4"))
  }

}
