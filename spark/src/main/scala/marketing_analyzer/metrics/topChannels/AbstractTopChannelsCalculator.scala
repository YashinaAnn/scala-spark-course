package marketing_analyzer.metrics.topChannels

import marketing_analyzer.Spark.spark
import marketing_analyzer.config.Configuration.config
import marketing_analyzer.utils.{DataReader, DataWriter, Validator}
import org.apache.spark.storage.StorageLevel

abstract class AbstractTopChannelsCalculator extends TopChannelsCalculator {

  def calculateMetric(projectionPath: String,
                      topChannelsPath: String): Unit = {

    Validator.checkFileExists(projectionPath)
    Validator.checkFileNotExists(topChannelsPath)

    val projectionDS = DataReader.readProjectionDS(projectionPath)
    val channels = calculate(projectionDS).persist(StorageLevel.MEMORY_ONLY_SER)
    channels.show()
    DataWriter.saveDS(topChannelsPath, channels)
  }

  def main(args: Array[String]): Unit = {
    calculateMetric(config.projectionPath, config.topChannelsPath)
    spark.close()
  }
}
