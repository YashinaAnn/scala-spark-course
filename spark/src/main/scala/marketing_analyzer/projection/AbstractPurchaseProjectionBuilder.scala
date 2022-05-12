package marketing_analyzer.projection

import marketing_analyzer.Spark.spark
import marketing_analyzer.config.Configuration.config
import marketing_analyzer.utils.{DataReader, DataWriter, Validator}
import org.apache.spark.sql.functions.{dayofmonth, month, year}

abstract class AbstractPurchaseProjectionBuilder extends PurchaseProjectionBuilder {

  def buildPurchaseProjection(clicksPath: String,
                              purchasesPath: String,
                              projectionPath: String): Unit = {

    Validator.checkFileExists(clicksPath)
    Validator.checkFileExists(purchasesPath)
    Validator.checkFileNotExists(projectionPath)

    val purchasesDS = DataReader.readPurchasesDS(purchasesPath)
    val clickStreamDS = DataReader.readClickStreamDS(clicksPath)
    val projectionDS = build(clickStreamDS, purchasesDS)
    val resDS = projectionDS
      .withColumn("year", year(projectionDS("purchaseTime")))
      .withColumn("month", month(projectionDS("purchaseTime")))
      .withColumn("date", dayofmonth(projectionDS("purchaseTime")))
    DataWriter.savePartitionedDS(projectionPath, resDS, Seq("year", "month", "date"))
  }


  def main(args: Array[String]): Unit = {
    buildPurchaseProjection(config.clickstreamPath, config.purchasesPath, config.projectionPath)
    spark.close()
  }
}
