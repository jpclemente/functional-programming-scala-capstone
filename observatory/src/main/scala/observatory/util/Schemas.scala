package observatory.util

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Schemas {
  val statSchema = new StructType(Array(
    StructField("stn", StringType, nullable = false),
    StructField("wban", StringType, nullable = true),
    StructField("latitude", StringType, nullable = true),
    StructField("longitude", StringType, nullable = true)
  ))

  val tempSchema = new StructType(Array(
    StructField("stn", StringType, nullable = false),
    StructField("wban", StringType, nullable = true),
    StructField("month", StringType, nullable = true),
    StructField("day", StringType, nullable = true),
    StructField("temperature", StringType, nullable = true)
  ))
}
