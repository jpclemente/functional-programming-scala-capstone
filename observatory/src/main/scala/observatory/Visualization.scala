package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface with SparkSessionWrapper {

  val width = 360
  val height = 180
  val alpha = 255
  val earthRadius = 6371000

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
                      else if (abs(lat1) == abs(lat2) && abs(lon1 - lon2) == Pi) Pi
                      else acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1))

    earthRadius * angDistance
  }

  case class A(upper: Double, lower: Double)

  var computed = 0

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val accValues = temperatures.foldLeft((0d, 0d)){
      (l, m) => {
        val d = correctedDistance(location, m._1)
        val result = (l._1 + m._2/d, l._2 + 1/ d)
        if (result._1.isNaN | result._2.isNaN) {
          throw new Exception("a NaN temperature has been predicted for location: " + location + ". \n Calculated distance between points is " + d)
        }
        else result
      }
    }
    accValues._1/accValues._2
  }

  def correctedDistance(a: Location, b: Location): Double = {
    val d = earthDistance(a, b)
    if (d < 1000d) 1
    else d
  }

  /**
    * Calculates the image of x through linear interpolation
    * @param p0 highest par (x0, y0)
    * @param p1 lowest value (x1, y1)
    * @param x value to interpolate. Must be between p0 and p1
    * @return image of x
    */
  def linearInterpolation(p0: (Double, Double), p1: (Double, Double), x: Double): Double = {
    val m = (p1._2 - p0._2) / (p1._1 - p0._1)
    p0._2 + m * (x - p0._1)
  }

  def interpolate(p0: (Double, Color), p1: (Double, Color), p: Double): Color = {
    val red = linearInterpolation((p0._1, p0._2.red), (p1._1, p1._2.red), p).toInt
    val green = linearInterpolation((p0._1, p0._2.green), (p1._1, p1._2.green), p).toInt
    val blue = linearInterpolation((p0._1, p0._2.blue), (p1._1, p1._2.blue), p).toInt

    Color(red, green, blue)
  }

  def valueInsideList(points: Iterable[Temperature], value: Temperature): Boolean = points.toList match {
    case Nil => throw new Exception("value can never be inside an empty list")
    case l => l.max >= value & value >= l.min
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val catalog = points.toList.sortBy(_._1)
    if (value >= catalog.reverse.head._1) catalog.reverse.head._2
    else if (value <= catalog.head._1) catalog.head._2
    else points.find(_._1 equals value) match {
        case None =>
          val p1 = catalog.filter(_._1 > value).minBy(_._1)
          val p0 = catalog.filter(_._1 < value).maxBy(_._1)
          interpolate(p0, p1, value)
        case Some(point) => point._2
      }
    }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val emptyFrame = for {x <-1 to width ; y <- 1 to height} yield (x, y)

    val a = emptyFrame
      .map(scaleLocations)
      .map(predictTemperature(temperatures,_))
      .map(interpolateColor(colors, _))
      .map(x => Pixel(x.red, x.green, x.blue, alpha))
      .toArray

    println("a.length = " + a.length)

    Image.apply(width, height, a)
  }

  def scaleLocations(cartesianCoords: (Int, Int)): Location = {
    val longitude = cartesianCoords._1 * width/360 - 180
    val latitude = cartesianCoords._2 * height/180 - 90
    Location(latitude, longitude)
  }
}

