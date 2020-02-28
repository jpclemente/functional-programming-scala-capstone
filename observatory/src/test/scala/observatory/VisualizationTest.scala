package observatory

import observatory.Visualization._
import observatory.Common._
import org.junit.Test

import scala.math._

trait VisualizationTest extends MilestoneSuite {

  val colors: Map[Temperature, Color] = Map(
    60d -> Color(255, 255, 255),
    32d -> Color(255, 0, 0),
    12d -> Color(255, 255, 0),
    0d -> Color(0, 255, 255),
    -15d -> Color(0, 0, 255),
    -27d -> Color(255, 0, 255),
    -50d -> Color(33, 0, 255),
    -60d -> Color(0, 0, 0)
  )

  private val milestoneTest = namedMilestoneTest("raw data display", 2) _

  // Implement tests for the methods of the `Visualization` object

  @Test
  def distanceTest(): Unit = {
    // Equivalent points
    assert(earthDistance(Location(0,0), Location(0,0)) equals 0d)
    assert(earthDistance(Location(90,90), Location(90,90)) equals 0d)

    // Antipodes
    assert(earthDistance(Location(60,90), Location(-60,-90)) equals Pi * earthRadius)
    assert(earthDistance(Location(-2,-90), Location(2,90)) equals Pi * earthRadius)
    assert(earthDistance(Location(0,-90), Location(0,90)) equals Pi * earthRadius)
    assert(earthDistance(Location(0,90), Location(0,-90)) equals Pi * earthRadius)
    assert(earthDistance(Location(58.0,-150.0), Location(-58.0,30.0)) equals Pi * earthRadius)

    //Other
    assert(earthDistance(Location(0,90), Location(90,0)) equals Pi/2 * earthRadius)
    assert(earthDistance(Location(90,90), Location(90,0)) equals 0d)
  }

  @Test
  def linearInterpTest(): Unit = {
    assert(linearInterpolation((0d, 0d), (2d, 2d), 1) equals 1d)
  }

  @Test(expected = classOf[Exception])
  def a(): Unit = {
    linearInterpolation((2d, 2d), (2d, 2d), _)
  }

  @Test
  def scaleLocationsTest(): Unit = {
    customAssert(Location(-90, -180), scaleLocations((0, 0)))
    customAssert( Location(0, 0), scaleLocations((width/2, height/2)))
    customAssert(Location(90, 180), scaleLocations((width, height)))
  }

  @Test
  def predictTemperatureTest(): Unit = {
    val temperatures = Seq((Location(0,2), 2d), (Location(1,1), 10d))
    customAssert(0, predictTemperature(temperatures, Location(0d,2d)))
    customAssert(0, predictTemperature(temperatures, Location(45d,90d)))
  }

  @Test
  def correctedDistanceTest(): Unit = {
    assert(correctedDistance(Location(0d, 0d), Location(0d, 0d)) equals 1d)
  }

  @Test
  def valueInsideListTest(): Unit = {
    customAssert(true, valueInsideList(List(0d, 2d), 0d))
    customAssert(true, valueInsideList(List(0d, 2d), 2d))
    customAssert(true, valueInsideList(List(0d, 2d), 1d))
    customAssert(false, valueInsideList(List(0d, 2d), 10d))
  }

  @Test
  def linearInterpolationTest(): Unit ={
    customAssert(1d, linearInterpolation((0d,0d), (2d,2d), 1d))
    customAssert(2d, linearInterpolation((0d,0d), (2d,2d), 2d))
    customAssert(0d, linearInterpolation((0d,0d), (2d,2d), 0d))
  }

  @Test
  def interpolateColorTest(): Unit = {
    customAssert(colors.maxBy(_._1)._2, interpolateColor(colors, colors.maxBy(_._1)._1 + 20))
    customAssert(colors.minBy(_._1)._2, interpolateColor(colors, colors.minBy(_._1)._1 - 20))
    customAssert(interpolate((-27d, colors(-27d)), (-15d, colors(-15d)), -20d), interpolateColor(colors, -20d))
    customAssert(interpolate((-27d, colors(-27d)), (-15d, colors(-15d)), -20d), interpolateColor(colors, -20d))
  }

  @Test
  def locationToColorTest(): Unit ={
    val temps = Seq()
    customAssert[Color](Color(255,0,0) , interpolateColor(colors, predictTemperature(temps, Location(90.0,-180.0))))
  }
}
