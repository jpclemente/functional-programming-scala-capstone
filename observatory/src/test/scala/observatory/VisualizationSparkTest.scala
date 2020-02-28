package observatory

import observatory.SparkVisualization._
import org.junit.Test

trait VisualizationSparkTest extends MilestoneSuite {

  private val milestoneTest = namedMilestoneTest("raw data display", 2) _

  // Implement tests for the methods of the `Visualization` object

  @Test
  def sparkVisualizeTest(): Unit = {
    val colors = Map(
      12d -> Color(255, 255, 0),
      0d -> Color(0, 255, 255),
      -15d -> Color(0, 0, 255),
    ).toSeq

    val locationTemperature = Seq((Location(2,2), 2d), (Location(1,10), 10d))

    sparkVisualize(locationTemperature, colors)
  }
}
