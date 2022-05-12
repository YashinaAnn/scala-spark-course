package marketing_analyzer.utils

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.{ClickEvent, ClickEventInput, DetailedPurchase, Purchase}
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}

object DataReader {

  val DefaultDateTimeFormat = "yyyy-MM-dd HH:mm:ss"

  def readClickStreamDS(path: String): Dataset[ClickEvent] = {
    readCsvDF(path, ClickEventInput.schema)
      .withColumn("eventTime",
        unix_timestamp($"eventTime", DefaultDateTimeFormat).cast(TimestampType))
      .as[ClickEventInput]
      .map(ClickEvent.fromClickEventInput)
  }

  def readPurchasesDS(path: String): Dataset[Purchase] = {
    readCsvDF(path, Purchase.schema)
      .withColumn("purchaseTime",
        unix_timestamp($"purchaseTime", DefaultDateTimeFormat).cast(TimestampType))
      .as[Purchase]
  }

  def readProjectionDS(path: String): Dataset[DetailedPurchase] = {
    spark.read
      .option("mergeSchema", "true")
      .parquet(path)
      .withColumn("purchaseTime",
        unix_timestamp($"purchaseTime", DefaultDateTimeFormat).cast(TimestampType))
      .as[DetailedPurchase]
  }

  def readCsvDF(path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(s"${path}/*.csv.gz")
  }

  def readParquetDF(path: String): DataFrame = {
    spark.read.parquet(path)
  }
}