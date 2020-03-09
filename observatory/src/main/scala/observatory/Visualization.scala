package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math._
import scala.util.Try

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val distFactor = 2
  val width = 360
  val height = 180
  val earthRadius = 6371

  def earthDistance(x: Location, y: Location): Double = {
    abs(earthRadius * angularDistance(x, y))
  }

  def angularDistance(x: Location, y: Location): Temperature = {
    // convert to radians

    if (x == y) 0
    else if (x.lat == -y.lat && abs(x.lon - y.lon) == 180) Pi
    else {
      val lat1 = x.lat.toRadians
      val lon1 = x.lon.toRadians
      val lat2 = y.lat.toRadians
      val lon2 = y.lon.toRadians
      acos(sin(lat1)*sin(lat2) + cos(lat1)*cos(lat2)*cos(abs(lon2-lon1)))
    }
  }
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    // change (Location, Temperature) -> (distance, Temperature)
    val distances = temperatures.map(x => (earthDistance(x._1, location), x._2))

    distances.filter(_._1 < 1).toList match {
      case Nil => computeTemperature(distances, location)
      case t => t.minBy(_._1)._2
    }
  }

  def computeTemperature(distances: Iterable[(Double, observatory.Temperature)], location: Location): Temperature = {
    Try{
      val weights = distances.map(x => (1/pow(x._1, distFactor), x._2))
      val down = weights.map(_._1).sum
      weights.map(x => x._1 * x._2).sum/down
    }.getOrElse(throw new Exception("An error occurred for location: " + location))
  }

  /**
    * Calculates the image of x through linear interpolation
    * @param p0 highest par (x0, y0)
    * @param p1 lowest value (x1, y1)
    * @param x value to interpolate. Must be between p0 and p1
    * @return image of x
    */
  def interpolate(p0: (Double, Color), p1: (Double, Color), x: Double): Color = {
    def linearInterpolation(y0: Double, y1: Double): Double = {
      val m = (y1 - y0) / (p1._1 - p0._1)
      y0 + m * (x - p0._1)
    }

    val red =   linearInterpolation(p0._2.red,    p1._2.red).round.toInt
    val green = linearInterpolation(p0._2.green,  p1._2.green).round.toInt
    val blue =  linearInterpolation(p0._2.blue,   p1._2.blue).round.toInt

    Color(red, green, blue)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {

    def greatest(it: Iterable[(Temperature, Color)]) = it.maxBy(_._1)
    def least(it: Iterable[(Temperature, Color)]) = it.minBy(_._1)

    //Check boundaries
    if (value > greatest(points)._1) greatest(points)._2
    else if (value < least(points)._1) least(points)._2

    else points.find(_._1 equals value) match {
        case None =>
          val p0 = greatest(points.filter(_._1 < value))
          val p1 = least(points.filter(_._1 > value))
          interpolate(p0, p1, value)
        case Some((_, color)) => color
      }
    }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val emptyFrame = for {x <-0 until height ; y <- 0 until width} yield (x, y)

    val pixels = emptyFrame
      .map(scaleLocations)
      .map(predictTemperature(temperatures,_))
      .map(interpolateColor(colors, _))
      .map(x => Pixel(x.red, x.green, x.blue, 255))
      .toArray

    println("a.length = " + pixels.length)

    Image(width, height, pixels)
  }

  /**
    * @param coord Pixel coordinates
    * @return Latitude and longitude
    */
  def scaleLocations(coord: (Int, Int)): Location = {
    val lon = (coord._2 - width/2) * 360/width
    val lat = -(coord._1 - height/2) * 180/height
    Location(lat, lon)
  }
}