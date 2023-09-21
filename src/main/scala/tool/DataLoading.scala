package tool

import core.DataCore
import org.apache.spark.sql.DataFrame

object DataLoading {

  def loadCsv(filename: String, seperator: String=",", inferSchema: String="false"): DataFrame ={
    DataCore.spark.read.format("csv")
      .option("sep",seperator)
      .option("inferSchema",inferSchema)
      .option("header","true")
      .load(filename)
  }

}
