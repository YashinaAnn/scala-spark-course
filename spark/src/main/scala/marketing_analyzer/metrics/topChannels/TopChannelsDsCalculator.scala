package marketing_analyzer.metrics.topChannels

import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.DetailedPurchase
import org.apache.spark.sql.Dataset

object TopChannelsDsCalculator extends AbstractTopChannelsCalculator {

  override def calculate(purchasesDS: Dataset[DetailedPurchase]): Dataset[(String, String)] = {
    purchasesDS
      .groupByKey(_.campaignId)
      .mapGroups { (campaignId, purchases) => {
        (campaignId, getTopChannel(purchases.toList))
      }}
  }

  private def getTopChannel(purchases: List[DetailedPurchase]): String = {
    purchases
      .groupBy(_.channelId)
      .mapValues { purchases => purchases.map(_.sessionId).distinct.size }
      .maxBy(_._2)._1
  }
}
