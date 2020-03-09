package observatory

import java.time._

import observatory.util.TimeRecord
import observatory.Visualization._


object Main extends App with SparkSessionWrapper {

  val year: Year = 2015
  val timeRecord = new TimeRecord()

  val outdir = "target/temperatures"

  val locationTemps = Extraction.locateTemperatures(year, "/stations.csv", "/" + year + ".csv")

  val tempsAvg = Extraction.locationYearlyAverageRecords(locationTemps)

  timeRecord.stepFinished(Instant.now(), "Extraction")

  val colors = Map[Temperature, Color](
    60d ->  Color(255,  255,  255),
    32d ->  Color(255,    0,    0),
    12d ->  Color(255,  255,    0),
    0d ->   Color(0,    255,  255),
    -15d -> Color(0,      0,  255),
    -27d -> Color(255,    0,  255),
    -50d -> Color(33,     0,  107),
    -60d -> Color(0,      0,    0)
  ).toSeq

  val result3 = visualize(tempsAvg, colors)
  timeRecord.stepFinished(Instant.now(), "Visualization")

  result3.output("image")
  timeRecord.processFinished(Instant.now())

}