package observatory

import java.time.LocalDate

import observatory.util.{Commons, Schemas}
import observatory.util.Commons._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface with SparkSessionWrapper {

  import spark.implicits._

  case class Date(year: Year, month: Int, day: Int)
  case class Record(year: Year, location: Location, temperature: Temperature)
  case class FinalRecord(location: Location, temperature: Temperature)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stations = spark.read.schema(Schemas.statSchema).csv(resourcePath(stationsFile))

    val temperatures = spark.read.schema(Schemas.tempSchema).csv(resourcePath(temperaturesFile))

    sparkLocateTemperatures(stations, temperatures, year)
  }

  def sparkLocateTemperatures(stations: DataFrame, temperatures: DataFrame, year: Year): Iterable[(LocalDate, Location, Temperature)] = {

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

    stats.join(temps, "id")
      .select("month", "day", "latitude", "longitude","temperature")
      .collect()
      .map(
        row => (
          LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
          Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude")),
          row.getAs[Temperature]("temperature")
        )
      )
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.map(x => Record(x._1.getYear, x._2, x._3)).toSeq.toDS()
      .groupBy($"year", $"location").mean("temperature")
      .select($"location", $"avg(temperature)" as "temperature")
      .as[(Location, Temperature)]
      .collect()
  }
}
