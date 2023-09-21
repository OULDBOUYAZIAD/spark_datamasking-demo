import org.apache.spark.sql.DataFrame

trait DataFrameTestUtils {

  def assertData(df1: DataFrame, df2: DataFrame): Boolean = {
    val data1 = df1.collect()
    val data2 = df2.collect()
    data1.diff(data2).isEmpty
  }
}
