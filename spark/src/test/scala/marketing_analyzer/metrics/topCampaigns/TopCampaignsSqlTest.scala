package marketing_analyzer.metrics.topCampaigns

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.DetailedPurchase
import marketing_analyzer.utils.TimeUtils

class TopCampaignsSqlTest extends AbstractTopCampaignsTest {

  override protected val calculator: TopCampaignsCalculator = TopCampaignsSqlCalculator

  test("Verify that empty list is returned for the top campaigns metric if there is no confirmed purchases") {
    val purchaseTime = TimeUtils.getTimestamp
    val df = spark.sparkContext.parallelize(
      List(
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "2", "8", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "1", "9", "Google Ads")
      )
    ).toDF.as[DetailedPurchase]

    val actualList = TopCampaignsSqlCalculator.calculate(10, df).collect().toList
    assert(List() == actualList)
  }
}
