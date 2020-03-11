package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {

  type Grid = Map[GridLocation, Temperature]

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    g: GridLocation => (for {i <- -89 to 90; j <- -180 to 179} yield Location(i, j))
      .map(x => (x, Visualization.predictTemperature(temperatures, x)))
      .toMap
      .getOrElse(Location(g.lat, g.lon), throw new Exception("Location not found"))
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    g: GridLocation => {
      val temps = temperaturess
        .map(makeGrid)
        .map(x => x(g))
      temps.sum/temps.size
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    g: GridLocation => makeGrid(temperatures)(g) - normals(g)
  }
}

