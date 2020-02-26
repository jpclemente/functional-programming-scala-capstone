package observatory

import java.time.LocalDate

import observatory.util.Schemas
import org.apache.spark.sql.Row
import org.junit.Test

trait ExtractionTest extends MilestoneSuite with SparkSessionWrapper {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  // Implement tests for the methods of the `Extraction` object

//  @Test
//  def sparkLocateTemperaturesBothEmpty(): Unit = {
//    val year = 2015
//    val expected = Array[Row]()
//
//    val stations = spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), Schemas.statSchema)
//
//    val temperatures = spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), Schemas.tempSchema)
//
//    val result = Extraction.sparkLocateTemperatures(stations, temperatures, year).collect()
//
//    assert(expected sameElements result, "both should have the same elements")
//
//    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
//    assert(difference.isEmpty, "difference should be empty")
//  }
//
//  @Test
//  def sparkLocateTemperaturesStationsEmpty(): Unit = {
//    val year = 2015
//    val expected = Array[Row]()
//
//    val stations = spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), Schemas.statSchema)
//
//    val temperatures = spark.createDataFrame(
//      spark.sparkContext.parallelize(
//        Seq(
//          Row("010013",      "","11","25", "39.2"),
//          Row("724017",      "","08","11","81.14"),
//          Row("724017", "03707","12","06",   "32"),
//          Row("724017", "03707","01","29", "35.6")
//        )
//      ), Schemas.tempSchema)
//
//    val result = Extraction.sparkLocateTemperatures(stations, temperatures, year).collect
//
//    assert(expected sameElements result, "both should have the same elements")
//
//    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
//    assert(difference.isEmpty, "both should have the same elements")
//  }
//
//  @Test
//  def sparkLocateTemperaturesTemperaturesEmpty(): Unit = {
//    val year = 2015
//    val expected = Array[Row]()
//
//    val stations = spark.createDataFrame(
//      spark.sparkContext.parallelize(
//        Seq(
//          Row("010013",      "",        "",        ""),
//          Row("724017", "03707", "+37.358","-078.438"),
//          Row("724017",      "", "+37.350","-078.433")
//        )
//      ), Schemas.statSchema)
//
//    val temperatures = spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), Schemas.tempSchema)
//
//    val result = Extraction.sparkLocateTemperatures(stations, temperatures, year).collect
//
//    assert(expected sameElements result, "both should have the same elements")
//
//    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
//    assert(difference.isEmpty, "both should have the same elements")
//  }
//
//  @Test
//  def sparkLocateTemperaturesBothNonEmpty(): Unit = {
//    val year = 2015
//    val expected = Array(
//      (LocalDate.of(year, 8, 11), Location(37.35, -78.433), 27.3d),
//      (LocalDate.of(year, 12, 6), Location(37.358, -78.438), 0.0d),
//      (LocalDate.of(year, 1, 29), Location(37.358, -78.438), 2.0d)
//    )
//
//    val stations = spark.createDataFrame(
//      spark.sparkContext.parallelize(
//        Seq(
//          Row("010013",      "",        "",        ""),
//          Row("724017", "03707", "+37.358","-078.438"),
//          Row("724017",      "", "+37.350","-078.433")
//        )
//      ), Schemas.statSchema)
//
//    val temperatures = spark.createDataFrame(
//      spark.sparkContext.parallelize(
//        Seq(
//          Row("010013",      "","11","25", "39.2"),
//          Row("724017",      "","08","11","81.14"),
//          Row("724017", "03707","12","06",   "32"),
//          Row("724017", "03707","01","29", "35.6")
//        )
//      ), Schemas.tempSchema)
//
//    val result = Extraction.sparkLocateTemperatures(stations, temperatures, year)
//      .collect()
//      .map(
//        row => (
//          LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
//          Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude")),
//          BigDecimal(row.getAs[Temperature]("temperature")).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
//        )
//      )
//
//    assert(expected sameElements result, "both should have the same elements")
//
//    val difference = result.foldLeft(expected)((e, r) => e.filter(_ equals r))
//    assert(difference.isEmpty, "both should have the same elements")
//  }
//
//  @Test
//  def locationYearlyAverageRecordsEmpty(): Unit = {
//    val input = Seq()
//    val expected = Seq()
//
//    val result = Extraction.locationYearlyAverageRecords(input).toSeq
//
//    assert(expected == result, "both should have the same elements")
//  }
//
//  @Test
//  def locationYearlyAverageRecordsNonEmpty(): Unit = {
//    val input = Seq(
//      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
//      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
//      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
//    )
//
//    val expected = Seq(
//      (Location(37.35, -78.433), 27.3),
//      (Location(37.358, -78.438), 1.0)
//    )
//
//    val result = Extraction.locationYearlyAverageRecords(input).toSeq
//
//    assert(expected == result, "both should have the same elements")
//  }
//
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