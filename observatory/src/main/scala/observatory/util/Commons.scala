package observatory.util

import observatory.Extraction.spark
import org.apache.spark.sql.{Column, Dataset}

import scala.io.Source
import scala.util.Try

object Commons {

  import spark.implicits._

  def getSourcePath(file: String): String = getClass.getResource(file).getPath

  def celsiusFromFahrenheit(f: Double): Double = (f - 32.0) * 5.0 / 9.0

  def getResource(path: String): Seq[Seq[String]] = {
    Source.fromInputStream(getClass.getResourceAsStream(path), "utf-8").getLines().map(x => (x split (",", -1)).toSeq).toSeq
  }

  def resourcePath(resource: String): Dataset[String] =
    Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().toSeq.toDS()

  def stringTo[T](field: String): Option[T] = {
    Try(field.asInstanceOf[T]).toOption
  }
}
