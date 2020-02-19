package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable{
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("my cool app")
      .getOrCreate()
  }

}
