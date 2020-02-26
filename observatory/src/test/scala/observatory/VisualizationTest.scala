package observatory

import observatory.Visualization._
import org.junit.Test

import scala.math._

trait VisualizationTest extends MilestoneSuite {

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
    assert(scaleLocations((0, 0)) equals Location(-90, -180))
    assert(scaleLocations((width/2, height/2)) equals Location(0, 0))
    assert(scaleLocations((width, height)) equals Location(90, 180))
  }

  @Test
  def predictTemperatureTest(): Unit = {
    val temperatures = Seq((Location(0,2), 2d), (Location(1,1), 10d))
    assert(predictTemperature(temperatures, Location(0,2)) equals 0)
  }

//  @Test
//  def sparkVisualizeTest(): Unit = {
//    val colors = Map(
//      12d -> Color(255, 255, 0),
//      0d -> Color(0, 255, 255),
//      -15d -> Color(0, 0, 255),
//    ).toSeq
//
//    val locationTemperature = Seq((Location(2,2), 2d), (Location(1,10), 10d))
//
//    val frame = for{x <- 1 to 360; y <- 1 to 180} yield (x, y)
//
//    sparkVisualize(locationTemperature, frame.toDS(), colors)
//  }
}
