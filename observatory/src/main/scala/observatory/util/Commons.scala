package observatory.util

import java.nio.file.Paths

import observatory.Extraction.{getClass, spark}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.io.Source

object Commons {

  import spark.implicits._

  def getSourcePath(file: String): String = getClass.getResource(file).getPath

  def celsiusFromFahrenheit(f: Column): Column = (f - 32.0) * 5.0 / 9.0

  def getResourceAsDF(file: String, schema: StructType): DataFrame = {
    spark.read
      .schema(schema)
      .csv(spark.sparkContext.parallelize(
        Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().toSeq)
        .toDS()
      )
  }

  def getResourceAsDS(file: String, s: Seq[String]): DataFrame = {
    spark.read
      .csv(spark.sparkContext.parallelize(s).toDS())
  }

  def resourcePath(resource: String): Dataset[String] =
    Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().toSeq.toDS()
}
