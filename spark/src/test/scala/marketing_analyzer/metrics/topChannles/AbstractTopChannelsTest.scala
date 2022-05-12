package marketing_analyzer.metrics.topChannles

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.metrics.topChannels.TopChannelsCalculator
import marketing_analyzer.model.DetailedPurchase
import marketing_analyzer.utils.TimeUtils
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractTopChannelsTest extends AnyFunSuite {

  protected val calculator: TopChannelsCalculator

  test("Verify top channels metric result") {
    val purchaseTime = TimeUtils.getTimestamp
    val df = spark.sparkContext.parallelize(
      List(
        DetailedPurchase("333", purchaseTime, Option(20.00), isConfirmed = true,
          "1", "9", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "1", "9", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "1", "9", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "2", "9", "Yandex Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "3", "9", "Yandex Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "2", "8", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "1", "8", "Google Ads"),
        DetailedPurchase("444", purchaseTime, Option(25.00), isConfirmed = false,
          "1", "8", "Yandex Ads")
      )
    ).toDF.as[DetailedPurchase]

    val actualList = calculator.calculate(df).collect().toList.sortBy(_._1)
    val expectedList = List(("8", "Google Ads"), ("9", "Yandex Ads"))

    assert(actualList == expectedList)
  }
}
