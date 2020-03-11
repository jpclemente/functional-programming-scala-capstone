package observatory

import java.io.File
import java.nio.file.{Files, Paths}
import java.time._

import observatory.util.TimeRecord


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


  def generateAndSaveTile(year: Year, tile: Tile, data: Iterable[(Location, Temperature)]): Unit = {
    val zoom = tile.zoom
    val x = tile.x
    val y = tile.y
    val zoomdir = f"$outdir%s/$year%d/$zoom%d"
    val fn = f"$zoomdir%s/$x%d-$y%d.png"
    Files.createDirectories(Paths.get(zoomdir))

    val img = Interaction.tile(data, colors, tile)
    img.output(new File(fn))
  }

  val data = List[(Year, Iterable[(Location, Temperature)])]((2015, tempsAvg))

  Interaction.generateTiles[Iterable[(Location, Temperature)]](data, generateAndSaveTile)
  timeRecord.stepFinished(Instant.now(), "Visualization")
  timeRecord.processFinished(Instant.now())


}