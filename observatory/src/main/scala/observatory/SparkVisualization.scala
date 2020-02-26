package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql.Dataset

import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object SparkVisualization extends VisualizationInterface with SparkSessionWrapper {

  val width = 360
  val height = 180
  val alpha = 255
  val earthRadius = 6371000

  import spark.implicits._

  /**
    * Computes the distance over the Earth surface between two points
    * @param x First point Location
    * @param y Second point Location
    * @return distance between x and y in metres
    */
  def earthDistance(x: Location, y: Location): Double = {
    // convert to radians
    val lat1 = x.lat.toRadians
    val lon1 = x.lon.toRadians
    val lat2 = y.lat.toRadians
    val lon2 = y.lon.toRadians

    val angDistance = if (lat1 == lat2 && lon1 == lon2) 0
                      else if (scala.math.abs(lat1) == scala.math.abs(lat2) && scala.math.abs(lon1 - lon2) == 180) Pi
                      else scala.math.acos( scala.math.sin(lat1) * scala.math.sin(lat2) + scala.math.cos(lat1) * scala.math.cos(lat2) * scala.math.cos(lon2 - lon1) )

    earthRadius * angDistance
  }

  case class A(upper: Double, lower: Double)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val accValues = temperatures.foldLeft((0d, 0d)){
      (l, m) => {
        val d = correctedDistance(location, m._1)
        (l._1 + m._2/ d, l._2 + 1/ d)
      }
    }
    if ((accValues._1/accValues._2).isNaN) throw new Exception("a NaN temperature has been predicted for location: " + Location)
    else accValues._1/accValues._2
  }

  def correctedDistance(a: Location, b: Location): Double = {
    val d = earthDistance(a, b)
    if (d < 1000) 1 else d
  }

  def linearInterpolation(p0: (Double, Double), p1: (Double, Double), x: Double): Double = {
    if (p0._1 equals p1._1) throw new Exception("Linear interpolation requires for two different points")
    else {
      val m = (p1._2 - p0._2) / (p1._1 - p0._1)
      p0._2 + m * (x - p0._1)
    }
  }

  def interpolate(p0: (Double, Color), p1: (Double, Color), p: Double): Color = {
    val red = linearInterpolation((p0._1, p0._2.red), (p1._1, p1._2.red), p).toInt
    val green = linearInterpolation((p0._1, p0._2.green), (p1._1, p1._2.green), p).toInt
    val blue = linearInterpolation((p0._1, p0._2.blue), (p1._1, p1._2.blue), p).toInt

    Color(red, green, blue)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    points.find(_._1 equals value) match {
      case None => points.filter(_._1 < value).toList match {
        case Nil => throw new Exception("Temperature out of color range. " + value + " must be between " + points.map(_._1).min + " and " + points.map(_._1).max)
        case l =>
          val right = points.filter(_._1 > value).minBy(_._1)
          interpolate(l.maxBy(_._1), right, value)
      }
      case Some(point) => point._2
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val emptyFrame = for {x <- 0 to width ; y <- 0 to height} yield (x, y)

    val a = emptyFrame
      .map(scaleLocations)
      .map(predictTemperature(temperatures,_))
      .map(interpolateColor(colors, _))
      .map(x => Pixel(x.red, x.green, x.blue, alpha))
      .toArray

    Image.apply(width, height, a)
  }

  def scaleLocations(cartesianCoords: (Int, Int)): Location = {
    val longitude = cartesianCoords._1 * 360/width - width/2
    val latitude = cartesianCoords._2 * 180/height - height/2
    Location(latitude, longitude)
  }
}

