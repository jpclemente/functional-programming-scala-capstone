package observatory

object Main extends App with SparkSessionWrapper {
  val result1 = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
  (result1 take 10) foreach println
}
