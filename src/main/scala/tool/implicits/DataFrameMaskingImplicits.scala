package tool.implicits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Those are the functions used for data masking policy
 */
object DataFrameMaskingImplicits {

  implicit class DataFrameMaskingImplicits(df: DataFrame) {
    /**
     * data masking functions, apply the specified policy to all rows within a column, based on the provided ColumnName.
     */

    /**
     *
     * @param columnName the name of the column
     * @return Mask the characters in the string, excluding the last 2 and the first 2 characters, with stars
     */
    def maskWithStars(columnName: String): DataFrame = {
      df.withColumn(columnName,
        concat_ws("",
          when(length(col(columnName)) < 4, col(columnName))
            .otherwise(expr(s"substring($columnName, 1, 2) || repeat('*', length($columnName) - 4) || substring($columnName, -2, 2)"))
        )
      )
    }

    /**
     *
     * @param columnName the name of the column
     * @return Extract only the year from a date element
     */
    def groupDateByYear(columnName: String): DataFrame = {
      df.withColumn(columnName,
        when(col(columnName).rlike("[0-9]{4}"), substring(col(columnName), 1, 4)))

    }

    /**
     *
     * @param columnName the name of the column
     * @return Concatenate a constant string to the value and then hash it using the SHA-2 algorithm
     */
    def hashValue(columnName: String): DataFrame = {
      val constant_str: String = "abc"
      df.withColumn(columnName,sha2(concat(col(columnName),lit(constant_str)),256))
    }

    /**
     *
     * @param columnName the name of the column
     * @return Retrieve the portion of the string located after the '@' character
     */
    def cutValue(columnName: String): DataFrame = {
      df.withColumn(columnName,
        when(col(columnName).rlike("@"), col(columnName).substr(instr(col(columnName), "@"), length(col(columnName))))
          .otherwise("")
      )
    }

    def dropValue(columnName: String): DataFrame = {
      df.withColumn(columnName, lit(""))
    }
  }
}