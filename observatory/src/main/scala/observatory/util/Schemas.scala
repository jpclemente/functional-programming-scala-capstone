package observatory.util

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Schemas {
  val locSchema = new StructType(Array(
    StructField("stn", StringType, nullable = false),
    StructField("wban", StringType, nullable = true),
    StructField("latitude", DoubleType, nullable = true),
    StructField("longitude", DoubleType, nullable = true)
  ))

  val tempSchema = new StructType(Array(
    StructField("stn", StringType, nullable = false),
    StructField("wban", StringType, nullable = true),
    StructField("month", IntegerType, nullable = true),
    StructField("day", IntegerType, nullable = true),
    StructField("temperature", DoubleType, nullable = true)
  ))
}
