package observatory

import java.time._

object Main extends App with SparkSessionWrapper {

  val year: Year = 2015

  val start = Instant.now()

  val result1 = Extraction.locateTemperatures(year, "/stations.csv", "/" + year + ".csv")

  val locationTemperature = Extraction.locationYearlyAverageRecords(result1)

  val time2 = Instant.now()
  println("Extraction completed in: " + Duration.between(start, time2).abs().getSeconds + " seconds")

  val colors = Map(
    60d -> Color(255, 255, 255),
    32d -> Color(255, 0, 0),
    12d -> Color(255, 255, 0),
    0d -> Color(0, 255, 255),
    -15d -> Color(0, 0, 255),
    -27d -> Color(255, 0, 255),
    -50d -> Color(33, 0, 255),
    -60d -> Color(0, 0, 0)
  ).toSeq

  val emptyFrame = for {x <- 1 to Visualization.width ; y <- 1 to Visualization.height} yield (x, y)

  val result3 = Visualization.visualize(locationTemperature, colors)

  val finalTime = Instant.now()
  println("Visualization completed in: " + Duration.between(time2, finalTime).abs().getSeconds + " seconds")

  result3.output("image")
  println("Total time elapsed: " + Duration.between(start, finalTime).abs().getSeconds + " seconds")
}