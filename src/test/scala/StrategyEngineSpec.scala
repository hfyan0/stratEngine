import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit._
import org.scalatest.mock._
import org.mockito.Mockito._

class StrategyEngineTest extends AssertionsForJUnit with MockitoSugar {
  @Test
  def testBogus() {
    assert(true)
  }

}
