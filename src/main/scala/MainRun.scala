import tool.{DataLoading, PolicyLoading}
import tool.implicits.DataFrameMaskingImplicits.DataFrameMaskingImplicits

object MainRun {

  def main(args: Array[String]): Unit = {

    val dataMaskingPolicy = PolicyLoading.loadJson("resources/mask.json")

    val df = DataLoading.loadCsv("resources/testing_data.csv")

    val transformedDF = dataMaskingPolicy.foldLeft(df) { case (currentDF, (key, value)) =>
      key match {
        case "hide_with_stars" => currentDF.maskWithStars(value)
        case "group_year" => currentDF.groupDateByYear(value)
        case "hash" => currentDF.hashValue(value)
        case "cut" => currentDF.cutValue(value)
        case "drop" => currentDF.dropValue(value)
        case _ => currentDF
      }
    }
    transformedDF.show()
  }
}