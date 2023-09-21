package tool

import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object PolicyLoading {

  def loadJson(policyFilePath: String): Map[String, String] = {
    val source:Source = null
    try {
      val source = Source.fromFile(policyFilePath)
      val policyFileContent = source.mkString
      implicit val formats: Formats = DefaultFormats
      val policyJson = parse(policyFileContent)
      policyJson.extract[Seq[JObject]].map { row =>
        val policy = (row \ "policy").extract[String]
        val column = (row \ "column").extract[String]
        policy -> column
      }.toMap
    }
      catch{
        case e: Exception => throw e

      }
    finally {
      if (source != null) {
        source.close()
      }
    }
  }
}
