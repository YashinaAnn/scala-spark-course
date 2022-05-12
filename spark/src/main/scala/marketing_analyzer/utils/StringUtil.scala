package marketing_analyzer.utils

object StringUtil {

  def parseStr2Map(str: String): Map[String, String] = {
    str.substring(1, str.length - 1)
      .replace(" ", "")
      .split(",")
      .map(_.split(":"))
      .map { case Array(k, v) =>
        val key = k.substring(1, k.length - 1)
        val value = if (v.startsWith("'"))
          v.substring(1, v.length - 1)
        else v
        (key, value)
      }.toMap
  }
}
