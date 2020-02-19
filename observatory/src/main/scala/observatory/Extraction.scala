package observatory

import java.time.LocalDate

import observatory.util.Commons._
import observatory.util.{Commons, Schemas}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface with SparkSessionWrapper {

  import spark.implicits._

  case class Record(year: Year, location: Location, temperature: Temperature)
  case class TemperatureRecord(id: String, year: Year, month: Int, day: Int, temperature: Temperature)
  case class Station(id: String, latitude: Double, longitude: Double)
  case class Date(year: Year, month: Int, day: Int)

  def toLocalDate(d: Date): LocalDate = LocalDate.of(d.year, d.month, d.day)

  private lazy val idCol = concat_ws("%", coalesce('_c0, lit("")), '_c1).alias("id")

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stations = spark.read.csv(resourcePath(stationsFile))
      .select(
        idCol,
        '_c2.alias("latitude").cast(DoubleType),
        '_c3.alias("longitude").cast(DoubleType)
      )
      .where('latitude.isNotNull && 'longitude.isNotNull && 'latitude =!= 0 && 'longitude =!= 0)
      .as[Station]

    val temperatures = spark.read.csv(resourcePath(temperaturesFile))
      .select(
        idCol,
        lit(year).alias("year"),
        '_c2.alias("month").cast(IntegerType),
        '_c3.alias("day").cast(IntegerType),
        '_c4.alias("temperature").cast(DoubleType)
      )
      .withColumn("temperature", Commons.celsiusFromFahrenheit($"temperature"))
      .as[TemperatureRecord]

    sparkLocateTemperatures(stations, temperatures, year)
      .collect()
      .map { row =>
        (LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
          Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude")),
          row.getAs[Temperature]("temperature"))
      }
  }

  def sparkLocateTemperatures[A, B](stations: Dataset[A], temperatures: Dataset[B], year: Year): DataFrame = {
    stations.join(temperatures, "id").persist()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    spark.createDataset(
      records.map(x => Record(x._1.getYear, x._2, x._3)).toSeq
    ).groupBy($"year", $"location").mean("temperature")
      .collect()
      .map { row => {
        val x = row.getStruct(1)
        (Location(x.getAs[Double]("lat"), x.getAs[Double]("lon")), row.getAs[Temperature]("avg(temperature)"))
        }
      }
  }
}
