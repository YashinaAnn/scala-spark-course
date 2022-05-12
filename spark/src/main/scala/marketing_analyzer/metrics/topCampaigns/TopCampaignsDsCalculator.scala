package marketing_analyzer.metrics.topCampaigns

import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.{CampaignRevenue, DetailedPurchase}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc

object TopCampaignsDsCalculator extends AbstractTopCampaignsCalculator {

  override def calculate(n: Int, purchases: Dataset[DetailedPurchase]): Dataset[(String, Double)] = {
    purchases
      .filter(_.isConfirmed)
      .map(purchase => CampaignRevenue(purchase.campaignId, purchase.billingCost.getOrElse(0)))
      .groupByKey(_.campaignId)
      .mapValues(_.revenue)
      .reduceGroups(_ + _)
      .toDF("campaignId", "revenue").as[(String, Double)]
      .sort(desc("revenue"))
      .limit(n)
  }
}
