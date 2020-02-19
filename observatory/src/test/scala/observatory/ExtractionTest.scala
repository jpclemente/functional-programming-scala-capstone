package observatory

import java.time.LocalDate

import observatory.Extraction.{Station, TemperatureRecord}
import org.apache.spark.sql.Dataset
import org.junit.Test

trait ExtractionTest extends MilestoneSuite with SparkSessionWrapper {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  // Implement tests for the methods of the `Extraction` object
  import spark.implicits._

  @Test
  def sparkLocateTemperaturesBothEmpty(): Unit = {
    val year = 2015
    val expected = Array[(LocalDate, Location, Temperature)]()

    val stations: Dataset[Station] = Seq[Station]().toDS()

    val temperatures: Dataset[TemperatureRecord] = Seq[TemperatureRecord]().toDS()

    val result = Extraction.sparkLocateTemperatures(stations, temperatures, 2015)
      .collect()
      .map { row =>
        (LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
          Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
          row.getAs[Temperature]("temp"))
      }

    assert(expected sameElements result, "both should have the same elements")

    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
    assert(difference.isEmpty, "both should have the same elements")
  }

  @Test
  def sparkLocateTemperaturesStationsEmpty(): Unit = {
    val year = 2015
    val expected = Array[(LocalDate, Location, Temperature)]()

    val stations: Dataset[Station] = Seq[Station]().toDS()

    val temperatures: Dataset[TemperatureRecord] = Seq(
          TemperatureRecord("010013",      2015, 11, 25, 39.2d),
          TemperatureRecord("724017",      2015,  8, 11, 27.3d),
          TemperatureRecord("72401703707", 2015, 12,  6, 0.0d),
          TemperatureRecord("72401703707", 2015,  1, 29, 2.0d)
        ).toDS()

    val result = Extraction.sparkLocateTemperatures(stations, temperatures, 2015)
      .collect()
      .map { row =>
        (LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
          Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
          row.getAs[Temperature]("temp"))
      }

    assert(expected sameElements result, "both should have the same elements")

    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
    assert(difference.isEmpty, "both should have the same elements")
  }

  @Test
  def sparkLocateTemperaturesTemperaturesEmpty(): Unit = {
    val year = 2015
    val expected = Array[(LocalDate, Location, Temperature)]()

    val stations: Dataset[Station] = Seq(
      Station("72401703707", +37.358d,-078.438d),
      Station("724017", +37.350d,-078.433d)
    ).toDS()

    val temperatures: Dataset[TemperatureRecord] = Seq[TemperatureRecord]().toDS()

    val result = Extraction.sparkLocateTemperatures(stations, temperatures, 2015)
      .collect()
      .map { row =>
        (LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
          Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
          row.getAs[Temperature]("temp"))
      }

    assert(expected sameElements result, "both should have the same elements")

    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
    assert(difference.isEmpty, "both should have the same elements")
  }

  @Test
  def sparkLocateTemperaturesBothNonEmpty(): Unit = {
    val year = 2015
    val expected = Array(
      (LocalDate.of(year, 8, 11), Location(37.35, -78.433), 27.3d),
      (LocalDate.of(year, 12, 6), Location(37.358, -78.438), 0.0d),
      (LocalDate.of(year, 1, 29), Location(37.358, -78.438), 2.0d)
    )

    val stations: Dataset[Station] = Seq(
      Station("72401703707", +37.358d,-078.438d),
      Station("724017", +37.350d,-078.433d)
    ).toDS()

    val temperatures: Dataset[TemperatureRecord] = Seq(
      TemperatureRecord("010013",      2015, 11, 25, 39.2d),
      TemperatureRecord("724017",      2015,  8, 11, 27.3d),
      TemperatureRecord("72401703707", 2015, 12,  6, 0.0d),
      TemperatureRecord("72401703707", 2015,  1, 29, 2.0d)
    ).toDS()

    val result = Extraction.sparkLocateTemperatures(stations, temperatures, 2015)
      .collect()
      .map { row =>
        (LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
          Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude")),
          row.getAs[Temperature]("temperature"))
      }

    assert(expected sameElements result, "both should have the same elements")

    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
    assert(difference.isEmpty, "both should have the same elements")
  }

  @Test
  def locationYearlyAverageRecordsEmpty(): Unit = {
    val input = Seq()
    val expected = Seq()

    val result = Extraction.locationYearlyAverageRecords(input).toSeq

    assert(expected == result, "both should have the same elements")
  }

  @Test
  def locationYearlyAverageRecordsNonEmpty(): Unit = {
    val input = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )

    val expected = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0)
    )

    val result = Extraction.locationYearlyAverageRecords(input).toSeq

    assert(expected == result, "both should have the same elements")
  }

//  @Test
//  def locateTemperaturesStationsEmpty(): Unit = {
//    val year = 2015
//
//    val expected = Array[(LocalDate, Location, Temperature)]()
//
//    val result = Extraction.locateTemperatures(year, "/empty.csv", "/"+year.toString+".csv").toArray
//
//    assert(expected sameElements result, "both should have the same elements")
//  }
//
//  @Test
//  def locateTemperaturesTemperaturesEmpty(): Unit = {
//    val year = 2015
//
//    val expected = Array[(LocalDate, Location, Temperature)]()
//
//    val result = Extraction.locateTemperatures(year, "/stations.csv", "/empty.csv").toArray
//
//    assert(expected sameElements result, "both should have the same elements")
//  }
}