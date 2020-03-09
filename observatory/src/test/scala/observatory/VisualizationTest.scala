package observatory

import observatory.Common._
import observatory.Visualization._
import org.junit.Test

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

  /*
  [Test Description] predicted temperature at location z should be closer to known temperature at location x than to
      known temperature at location y, if z is closer (in distance) to x than y, and vice versa (10pts)
      (observatory.CapstoneSuite)
  [Observed Error] assertion failed: Incorrect predicted temperature at Location(45.0,90.0): 16.923076923076923.
      Expected to be closer to 10.0 than 20.0

  [Test Description] visualize (5pts)(observatory.CapstoneSuite)
  [Observed Error] Incorrect computed color at Location(90.0,-180.0): Color(26,0,230). Expected to be closer to Color(255,0,0) than Color(0,0,255)
  * */

  // Implement tests for the methods of the `Visualization` object

  @Test
  def scaleLocationsTest(): Unit = {
    customAssert(Location(90, -180), scaleLocations((0, 0)))
    customAssert( Location(0d, 0d), scaleLocations((height/2, width/2)))
    customAssert(Location(-90, 180), scaleLocations((height, width)))
  }

  @Test
  def predictTemperatureTest(): Unit = {
    val temperatures = Seq(
      (Location(0,0), 2d),
      (Location(-0.0001,0), -20d),
      (Location(3,3), 4d),
      (Location(6,6), 8d),
      (Location(10,10), 100d)
    )
    customAssert(2d, predictTemperature(temperatures, Location(0d,0d)))
    customAssert(-20d, predictTemperature(temperatures, Location(-0.005d,0d)))

    val location2 = Location(45d,90d)
    val expected = computeTemperature(temperatures.map(x => (earthDistance(x._1, location2),x._2)), location2)

    customAssert(expected, predictTemperature(temperatures, location2))
  }

  @Test
  def predictTemperaturesAtPoles(): Unit ={
    val temperatures = Seq(
      (Location(-90,0), 2d),
      (Location(-0.0001,0), -20d),
      (Location(3,3), 4d),
      (Location(6,6), 8d),
      (Location(90,10), 100d)
    )
    customAssert(2d, predictTemperature(temperatures,Location(-90, 30)))
    customAssert(2d, predictTemperature(temperatures,Location(-90, -180)))
    customAssert(100d, predictTemperature(temperatures,Location(90, 30)))
    customAssert(100d, predictTemperature(temperatures,Location(90, 180)))
  }

  @Test
  def angularDistanceTest(): Unit = {
    customAssert(0d, angularDistance(Location(90, 0), Location(90, 100)))
    customAssert(0d, angularDistance(Location(90, 0), Location(90, 180)))
    customAssert(0d, angularDistance(Location(90, 0), Location(90, -180)))
    customAssert(0d, angularDistance(Location(-90, 0), Location(-90, 100)))
    customAssert(0d, angularDistance(Location(-90, 0), Location(-90, 180)))
    customAssert(0d, angularDistance(Location(-90, 0), Location(-90, -180)))
  }

  @Test
  def interpolateColorTest(): Unit = {
    customAssert(colors.maxBy(_._1)._2, interpolateColor(colors, colors.maxBy(_._1)._1 + 20))
    customAssert(colors.minBy(_._1)._2, interpolateColor(colors, colors.minBy(_._1)._1 - 20))
    customAssert(interpolate((-27d, colors(-27d)), (-15d, colors(-15d)), -20d), interpolateColor(colors, -20d))
    customAssert(interpolate((-27d, colors(-27d)), (-15d, colors(-15d)), -20d), interpolateColor(colors, -20d))
  }

  @Test
  def interpolateTest(): Unit = {
    val p0 = (0d, Color(0, 0, 0))
    val p1 = (10d, Color(255, 255, 0))

    customAssert[Color](Color(153, 153, 0), interpolate(p0, p1, 6d))
  }

  @Test
  def locationToColorTest(): Unit = {
    val temperatures = Seq(
      (Location(-90,0), 0d),
      (Location(-45,180), 10d),
      (Location(0,0), 20d),
      (Location(45,180), 10d),
      (Location(90,0), 0d)
    )

    def compute(location: Location): Color = {
      val temp = predictTemperature(temperatures, location)
      println("predicted temp: " + temp)
      interpolateColor(colors, temp)
    }

    customAssert[Color](Color(0,255,255), compute(Location(90.0,-180.0)))
    customAssert[Color](Color(124,255,131), compute(Location(45.0,90.0)))
    customAssert[Color](Color(124,255,131), compute(Location(45.0,-90.0)))
    customAssert[Color](Color(0,255,255), compute(Location(-90.0,-180.0)))
  }
}
