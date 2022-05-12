package marketing_analyzer.metrics.topCampaigns

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.DetailedPurchase
import marketing_analyzer.utils.TimeUtils
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractTopCampaignsTest extends AnyFunSuite  {

  protected val calculator: TopCampaignsCalculator

  test("Verify top campaigns metric result") {
    val purchaseTime = TimeUtils.getTimestamp
    val df = spark.sparkContext.parallelize(
      List(
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "2", "8", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "1", "8", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = true,
          "1", "8", "Yandex Ads"),
        DetailedPurchase("333", purchaseTime, Option(20.00), isConfirmed = true,
          "1", "7", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = true,
          "1", "7", "Yandex Ads"),
        DetailedPurchase("444", purchaseTime, Option(150.00), isConfirmed = true,
          "1", "10", "Yandex Ads")
      )
    ).toDF.as[DetailedPurchase]

    val actualList = calculator.calculate(3, df).collect().toList
    val expectedList = List(("10", 150.00), ("7", 45.00), ("8", 25.00))

    assert(expectedList == actualList)
  }

  test("Verify that only confirmed purchases are considered for the Top Campaigns metric") {
    val purchaseTime = TimeUtils.getTimestamp
    val df = spark.sparkContext.parallelize(
      List(
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "2", "8", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = true,
          "1", "9", "Google Ads")
      )
    ).toDF.as[DetailedPurchase]

    val actualList = calculator.calculate(10, df).collect().toList
    val expectedList = List(("9", 25.00))
    assert(expectedList == actualList)
  }
}
