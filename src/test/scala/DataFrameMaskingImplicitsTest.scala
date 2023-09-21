import core.DataCore
import org.scalatest.funsuite.AnyFunSuite
import tool.DataLoading
import tool.implicits.DataFrameMaskingImplicits.DataFrameMaskingImplicits
import org.apache.spark.sql.functions._
import DataCore.spark.implicits._
class DataFrameMaskingImplicitsTest extends AnyFunSuite with DataFrameTestUtils {

test("test_maskWithStars") {
  val df = DataLoading.loadCsv("resources/data_for_test.csv")
  val result =df.maskWithStars("credit_card_number")
  val expectedDf = Seq(
    ("ziad","ziad@gmail.com","2000-05-08","29****13","680092094"),
    ("hakim","hakim@gmail.com","2000-04-06","13****19","680094322"),
  ).toDF("name","email","birth_date","credit_card_number","phone_number")
  assert(assertData(expectedDf,result))
}

  test("test_groupDateByYear") {
    val df = DataLoading.loadCsv("resources/data_for_test.csv")
    val result =df.groupDateByYear("birth_date")
    val expectedDf = Seq(
      ("ziad","ziad@gmail.com","2000","29329113","680092094"),
      ("hakim","hakim@gmail.com","2000","13131319","680094322"),
    ).toDF("name","email","birth_date","credit_card_number","phone_number")
    assert(assertData(expectedDf,result))
  }

  test("test_cutValue") {
    val df = DataLoading.loadCsv("resources/data_for_test.csv")
    val result = df.cutValue("email")
    val expectedDf = Seq(
      ("ziad", "@gmail.com", "2000-05-08", "29329113", "680092094"),
      ("hakim", "@gmail.com", "2000-04-06", "13131319", "680094322"),
    ).toDF("name", "email", "birth_date", "credit_card_number", "phone_number")
    assert(assertData(expectedDf, result))
  }







}
