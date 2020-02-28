package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql.Dataset
import observatory.Visualization._

import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object SparkVisualization extends SparkSessionWrapper {

  val width = 360
  val height = 180
  val alpha = 255
  val earthRadius = 6371000

  import spark.implicits._

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def sparkVisualize(temperatures: Iterable[(Location, Temperature)],
                     colors: Iterable[(Temperature, Color)]): Image = {
    val emptyFrame = for {x <- 0 to width ; y <- 0 to height} yield (x, y)

    val a = emptyFrame.toDS()
      .map(scaleLocations)
      .map(predictTemperature(temperatures,_))
      .map(interpolateColor(colors, _))
      .map(x => Pixel(x.red, x.green, x.blue, alpha))
      .collect()

    Image.apply(width, height, a)
  }
}

