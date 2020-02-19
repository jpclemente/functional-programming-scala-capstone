package observatory.util

import observatory.{MilestoneSuite, SparkSessionWrapper}
import org.junit.Test

class CommonsTest extends MilestoneSuite with SparkSessionWrapper {

  import spark.implicits._

  @Test
  def celsiusFromFahrenheit(): Unit = {
    val expected = Array((32, 0),(41, 5))

    val input = Seq(32, 41).toDF("fahrenheit")

    val result = input.withColumn("celsious", Commons.celsiusFromFahrenheit($"fahrenheit"))
      .collect.map(row => (row.get(0), row.get(1)))

    assert(result sameElements expected)
  }
}
