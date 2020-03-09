package observatory

import java.time.LocalDate
import observatory.Common._

import org.junit.Test

trait ExtractionTest extends MilestoneSuite with SparkSessionWrapper {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  // Implement tests for the methods of the `Extraction` object

  @Test
  def locateTemperaturesBothNonEmpty(): Unit = {
    val year = 2015
    val expected = Seq(
      (LocalDate.of(year, 1, 1), Location(-90, -180), 0.0),
      (LocalDate.of(year, 2, 2), Location(0, 0), 5.0d),
      (LocalDate.of(year, 3, 3), Location(90, 180), -20.0d)
    )

    val result = Extraction.locateTemperatures(year, "/stations.csv", "/"+year.toString+".csv").toSeq

    customAssert(expected, result)

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

  @Test
  def locateTemperaturesStationsEmpty(): Unit = {
    val year = 2015

    val expected = Array[(LocalDate, Location, Temperature)]()

    val result = Extraction.locateTemperatures(year, "/empty.csv", "/"+year.toString+".csv").toArray

    assert(expected sameElements result, "both should have the same elements")
  }

  @Test
  def locateTemperaturesTemperaturesEmpty(): Unit = {
    val year = 2015

    val expected = Array[(LocalDate, Location, Temperature)]()

    val result = Extraction.locateTemperatures(year, "/stations.csv", "/empty.csv").toArray

    assert(expected sameElements result, "both should have the same elements")
  }
}