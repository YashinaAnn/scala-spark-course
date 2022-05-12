package marketing_analyzer.metrics.topCampaigns

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.DetailedPurchase
import org.apache.spark.sql.Dataset

object TopCampaignsSqlCalculator extends AbstractTopCampaignsCalculator {

  override def calculate(n: Int, purchases: Dataset[DetailedPurchase]): Dataset[(String, Double)] = {
    purchases.createOrReplaceTempView("purchases")
    spark.sql(
      s"""
          SELECT campaignId, SUM(billingCost) AS revenue
          FROM (SELECT * FROM purchases WHERE isConfirmed = TRUE)
          GROUP BY campaignId
          ORDER BY revenue DESC
          LIMIT $n
      """.stripMargin)
      .as[(String, Double)]
  }
}
