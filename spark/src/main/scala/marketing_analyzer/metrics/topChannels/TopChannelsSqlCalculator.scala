package marketing_analyzer.metrics.topChannels

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.DetailedPurchase
import org.apache.spark.sql.Dataset

object TopChannelsSqlCalculator extends AbstractTopChannelsCalculator {

  override def calculate(purchases: Dataset[DetailedPurchase]): Dataset[(String, String)] = {
    purchases.createOrReplaceTempView("purchases")
    spark.sql(
      s"""
        WITH t_counts AS (
          SELECT campaignId, channelId, count(*) AS counts
            FROM (
              SELECT DISTINCT campaignId, channelId, sessionId
              FROM purchases
            ) sessions
            GROUP BY campaignId, channelId
          )
          SELECT T1.campaignId, channelId
            FROM t_counts T1
            WHERE NOT EXISTS (
              SELECT channelId
              FROM t_counts T2
              WHERE T2.counts > T1.counts AND T2.campaignId = T1.campaignId
            )
      """.stripMargin)
      .as[(String, String)]
  }
}
