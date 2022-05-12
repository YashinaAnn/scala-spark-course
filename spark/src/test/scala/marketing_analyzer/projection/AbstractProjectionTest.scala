package marketing_analyzer.projection

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model._
import marketing_analyzer.utils.TimeUtils
import org.scalatest.funsuite.AnyFunSuite


abstract class AbstractProjectionTest extends AnyFunSuite {

  protected val projectionBuilder: PurchaseProjectionBuilder

  test("Verify purchase projection result for the single user") {
    val purchaseTime = TimeUtils.getTimestamp
    val purchase = DetailedPurchase("333", purchaseTime, Option(20.00), isConfirmed = true, "", "9", "Google Ads")

    val events = List(
      ClickEvent("1", "1", EventType.AppOpen.name, TimeUtils.minusMinutes(purchaseTime, 10),
        Option(Map(EventAttribute.CampaignId -> purchase.campaignId, EventAttribute.ChannelId -> purchase.channelId))),
      ClickEvent("1", "2", EventType.Purchase.name, purchaseTime,
        Option(Map(EventAttribute.PurchaseId -> purchase.purchaseId))),
      ClickEvent("1", "3", EventType.AppClose.name, TimeUtils.plusMinutes(purchaseTime, 10), None)
    )
    val purchases = List(
      Purchase(purchase.purchaseId, purchaseTime, purchase.billingCost.get, isConfirmed = true)
    )

    val eventsDS = spark.sparkContext.parallelize(events).toDS().as[ClickEvent]
    val purchasesDS = spark.sparkContext.parallelize(purchases).toDS().as[Purchase]

    val result = projectionBuilder.build(eventsDS, purchasesDS).collect().toList

    assert(result == List(purchase.copy(sessionId = result.head.sessionId)))
  }

  test("Verify purchase projection result for multiple users") {
    val purchaseTime1 = TimeUtils.getTimestamp
    val purchaseTime2 = TimeUtils.plusMinutes(purchaseTime1, 2)

    val events = List(
      ClickEvent("1", "1", EventType.AppOpen.name, TimeUtils.minusMinutes(purchaseTime1, 10),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("1", "2", EventType.Purchase.name, purchaseTime1,
        Option(Map(EventAttribute.PurchaseId -> "333"))),
      ClickEvent("1", "4", EventType.AppClose.name, TimeUtils.plusMinutes(purchaseTime1, 10),
        None),
      ClickEvent("2", "11", EventType.AppOpen.name, TimeUtils.minusMinutes(purchaseTime2, 10),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("2", "12", EventType.Purchase.name, purchaseTime2,
        Option(Map(EventAttribute.PurchaseId -> "444"))),
      ClickEvent("2", "14", EventType.AppClose.name, TimeUtils.plusMinutes(purchaseTime2, 10),
        None)
    )

    val purchases = List(
      Purchase("333", purchaseTime1, 20.00, isConfirmed = true),
      Purchase("444", purchaseTime2, 25.00, isConfirmed = false)
    )

    val eventsDS = spark.sparkContext.parallelize(events).toDS().as[ClickEvent]
    val purchasesDS = spark.sparkContext.parallelize(purchases).toDS().as[Purchase]

    val result = projectionBuilder.build(eventsDS, purchasesDS).collect().toList.sortBy(_.purchaseId)

    val expected = List(
      DetailedPurchase("333", purchaseTime1, Option(20.00), isConfirmed = true,
        result.head.sessionId, "9", "Google Ads"),
      DetailedPurchase("444", purchaseTime2, Option(25.00), isConfirmed = false,
        result.tail.head.sessionId, "9", "Google Ads")
    )

    assert(result == expected)
  }
}