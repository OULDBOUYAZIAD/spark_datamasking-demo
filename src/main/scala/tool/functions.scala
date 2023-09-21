package tool
import org.apache.spark.sql.functions._
object functions {


  def hideWithStars(rowValue : String):String = {
    val n = rowValue.length
    if (n < 4) {
      val stars = "*" * n
      stars
    }
    else {
      val stars = "*" * (n-4)
      val result = rowValue.substring(0,2) + stars + rowValue.substring(n-2,n)
      result
    }

  }

  def groupDateByYear(rowValue : String): String = {
    val dateList = rowValue.split("[\\-/]")
    val sortedDate = dateList.sortBy(_.length)(Ordering.Int.reverse)
    val gropingValue = sortedDate.headOption.getOrElse("")
    gropingValue
  }

  def hashValue(rowValue: String): String = {
    val result = rowValue.hashCode()
    result.toString

  }

  def cutValue(rowValue: String): String = {
    val elements = rowValue.split("@")
    if (elements.length >= 2){
      "@" + elements.last
    }else {
      ""
    }
  }

  def dropValue(rowValue: String): String = {
    val result = ""
    result
  }


}
