package marketing_analyzer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .appName("MarketingAnalyzer")
    .master("local[*]")
    .getOrCreate()
}