package observatory.util

import observatory.{MilestoneSuite, SparkSessionWrapper}
import observatory.util.Commons._
import observatory.Common._
import org.junit.Test

class CommonsTest extends MilestoneSuite with SparkSessionWrapper {
  @Test
  def celsiusFromFahrenheitTest(): Unit = {
    customAssert(0d, celsiusFromFahrenheit(32d))
  }
}
