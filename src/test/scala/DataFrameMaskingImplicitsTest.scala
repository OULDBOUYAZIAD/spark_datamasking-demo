import org.scalatest.funsuite.AnyFunSuite
import tool.DataLoading
import tool.implicits.DataFrameMaskingImplicits.DataFrameMaskingImplicits

class DataFrameMaskingImplicitsTest extends AnyFunSuite {



  test("test_groupByYear") {
    val year = "08-05-2000"
    val expected = "2000"
    val actual = tool.functions.groupDateByYear(year)
    assert(expected == actual)
  }

  test("test_cut") {
    val email = "ziad.ould.bouya@nemo.ma"
    val expected = "@nemo.ma"
    val actual = tool.functions.cutValue(email)
    assert(expected == actual)
  }




}
