package core

import org.apache.spark.sql.{SparkSession,functions}

object DataCore{

  val spark: SparkSession =
    SparkSession
     .builder()
     .appName("datamasking")
     .master("local[*]")
      .getOrCreate()
}