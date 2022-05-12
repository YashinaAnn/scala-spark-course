package marketing_analyzer.metrics.topCampaigns

import marketing_analyzer.Spark.spark
import marketing_analyzer.config.Configuration.config
import marketing_analyzer.utils.{DataReader, DataWriter, Validator}
import org.apache.spark.storage.StorageLevel

abstract class AbstractTopCampaignsCalculator extends TopCampaignsCalculator {

  def calculateMetric(topLimit: Int,
                      projectionPath: String,
                      topCampaignsPath: String): Unit = {

    Validator.checkFileExists(projectionPath)
    Validator.checkFileNotExists(topCampaignsPath)

    val projectionDS = DataReader.readProjectionDS(projectionPath)
    val campaigns = calculate(topLimit, projectionDS).persist(StorageLevel.MEMORY_ONLY_SER)
    campaigns.show()
    DataWriter.saveDS(topCampaignsPath, campaigns)
  }

  def main(args: Array[String]): Unit = {
    calculateMetric(config.topCampaignsLimit, config.projectionPath, config.topCampaignsPath)
    spark.close()
  }
}
