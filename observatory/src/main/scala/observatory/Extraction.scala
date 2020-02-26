package observatory

import java.time.LocalDate

import observatory.util.Commons._
import observatory.util.{Commons, Schemas}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface with SparkSessionWrapper {

  import spark.implicits._

  case class Date(year: Year, month: Int, day: Int)
  case class Record(year: Year, location: Location, temperature: Temperature)
  case class Station(id: String, lat: Double, long: Double)
  case class TemperatureRecord(id: String, month: Int, day: Int, temp: Temperature)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stations: Map[String, Location] = getResource(stationsFile)
      .flatMap(x => Try(Station(x.head + x(1), x(2).toDouble, x(3).toDouble)).toOption)
      .map(x => (x.id, Location(x.lat, x.long)))
      .toMap

    val temperatures: Seq[TemperatureRecord] = getResource(temperaturesFile)
      .flatMap(x => Try(TemperatureRecord(x.head + x(1), x(2).toInt, x(3).toInt, x(4).toDouble)).toOption)

    temperatures.flatMap(toLocationTemp(year, stations, _))
  }

  private def toLocationTemp(year: Year, stations: Map[String, Location], temp: TemperatureRecord): Option[(LocalDate, Location, Double)] = {
    Try(LocalDate.of(year, temp.month, temp.day), stations(temp.id), temp.temp).toOption
  }

  def sparkLocateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {

    val stations = spark.read.schema(Schemas.statSchema).csv(resourcePath(stationsFile))

    val temperatures = spark.read.schema(Schemas.tempSchema).csv(resourcePath(temperaturesFile))

    lazy val id = concat_ws("%", coalesce($"stn", lit("")), $"wban") as "id"

    val temps = temperatures
      .select(
        id,
        lit(year)         as "year",
        $"month"        cast IntegerType,
        $"day"          cast IntegerType,
        $"temperature"  cast DoubleType
      )
      .withColumn("temperature", Commons.celsiusFromFahrenheit($"temperature"))

    val stats = stations
      .select(
        id,
        $"latitude" cast DoubleType,
        $"longitude" cast DoubleType
      )
      .where('latitude.isNotNull && 'longitude.isNotNull && 'latitude =!= 0 && 'longitude =!= 0)

    stats.join(temps, "id").select("month", "day", "latitude", "longitude","temperature")
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records
      .groupBy(r => (r._1.getYear, r._2))
      .map(x => (x._1._2, x._2.map(y => y._3).sum/x._2.count(_ => true)))
  }

  def sparkLocationYearlyAverageRecords(records: Dataset[Record]): Dataset[(Location, Temperature)] = {
    records.groupBy($"year", $"location")
      .mean("temperature")
      .select($"location", $"avg(temperature)" as "temperature")
      .as[(Location, Temperature)]
  }
}
