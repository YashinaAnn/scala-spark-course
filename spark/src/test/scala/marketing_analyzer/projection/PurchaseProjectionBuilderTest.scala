package marketing_analyzer.projection

import marketing_analyzer.Spark.spark
import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model._
import marketing_analyzer.projection.PurchaseProjectionDsBuilder.{getUserPurchaseAttributes, extractAttributes}
import marketing_analyzer.utils.TimeUtils

import java.sql.Timestamp

class PurchaseProjectionBuilderTest extends AbstractProjectionTest {

  override protected val projectionBuilder: PurchaseProjectionBuilder = PurchaseProjectionDsBuilder

  test("Verify getUserPurchaseAttributes result") {
    val purchaseTime = TimeUtils.getTimestamp

    val data = List(
      ClickEvent("1", "1", EventType.AppOpen.name, TimeUtils.minusMinutes(purchaseTime, 10),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("1", "2", EventType.Purchase.name, purchaseTime,
        Option(Map(EventAttribute.PurchaseId -> "333")))
    )
    val expected = PurchaseAttributes("333", null, "9", "Google Ads")
    val actualList = getUserPurchaseAttributes(data)

    assert(1 == actualList.size)
    val actual = actualList.head
    assert(actual == expected.copy(sessionId = actual.sessionId))
  }

  test("Verify getUserPurchaseAttributes result for the reversed events order") {
    val purchaseTime = TimeUtils.getTimestamp

    val data = List(
      ClickEvent("1", "2", EventType.Purchase.name, purchaseTime,
        Option(Map(EventAttribute.PurchaseId -> "333"))),
      ClickEvent("1", "1", EventType.AppOpen.name, TimeUtils.minusMinutes(purchaseTime, 10),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads")))
    )
    val expected = PurchaseAttributes("333", "", "9", "Google Ads")
    val actualList = getUserPurchaseAttributes(data)

    assert(1 == actualList.size)
    val actual = actualList.head

    assert(actual.id == expected.id)
    assert(actual == expected.copy(sessionId = actual.sessionId))
  }

  test("Verify getUserPurchaseAttributes result for the multiple sessions per user") {
    val data = List(
      ClickEvent("1", "1", EventType.AppOpen.name, Timestamp.valueOf("2021-07-26 08:12:13"),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("1", "2", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 10:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "333"))),
      ClickEvent("1", "11", EventType.AppOpen.name, Timestamp.valueOf("2021-07-29 08:12:13"),
        Option(Map(EventAttribute.CampaignId -> "98", EventAttribute.ChannelId -> "Yandex Ads"))),
      ClickEvent("1", "21", EventType.Purchase.name, Timestamp.valueOf("2021-07-29 10:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "444"))),
    )
    val expectedList = List(
      PurchaseAttributes("333", "", "9", "Google Ads"),
      PurchaseAttributes("444", "", "98", "Yandex Ads"))

    val actualList = getUserPurchaseAttributes(data).sortBy(_.id)

    assert(2 == actualList.size)
    assert(actualList.head.sessionId != actualList(1).sessionId)

    actualList.zip(expectedList).foreach(x =>
      assert(x._1 == x._2.copy(sessionId = x._1.sessionId))
    )
  }

  test("Verify getUserPurchaseAttributes result for the single session per user") {
    val data = List(
      ClickEvent("1", "1", EventType.AppOpen.name, Timestamp.valueOf("2021-07-26 08:12:13"),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("1", "2", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 10:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "333"))),
      ClickEvent("1", "3", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 11:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "444"))),
    )
    val expectedList = List(
      PurchaseAttributes("333", "", "9", "Google Ads"),
      PurchaseAttributes("444", "", "9", "Google Ads"))

    val actualList = getUserPurchaseAttributes(data)

    assert(2 == actualList.size)
    assert(actualList.head.sessionId == actualList(1).sessionId)
    actualList.zip(expectedList).foreach(x =>
      assert(x._1 == x._2.copy(sessionId = x._1.sessionId))
    )
  }

  test("Verify extractAttributes result for the single session per user") {
    val events = List(
      ClickEvent("1", "1", EventType.AppOpen.name, Timestamp.valueOf("2021-07-26 08:12:13"),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("2", "14", EventType.AppOpen.name, Timestamp.valueOf("2021-07-26 08:13:13"),
        Option(Map(EventAttribute.CampaignId -> "89", EventAttribute.ChannelId -> "Yandex Ads"))),
      ClickEvent("2", "24", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 10:10:13"),
        Option(Map(EventAttribute.PurchaseId -> "444"))),
      ClickEvent("1", "2", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 10:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "333"))),
      ClickEvent("1", "3", EventType.AppClose.name, Timestamp.valueOf("2021-07-26 15:12:13"),
        None),
      ClickEvent("2", "34", EventType.AppClose.name, Timestamp.valueOf("2021-07-26 20:12:13"),
        None)
    )

    val ds = spark.sparkContext.parallelize(events).toDS().as[ClickEvent]
    val resDS = extractAttributes(ds)
    val actual = resDS.collect().sortBy(_.id).toList

    val expected = List(
      PurchaseAttributes("333", "", "9", "Google Ads"),
      PurchaseAttributes("444", "", "89", "Yandex Ads")
    )

    actual.zip(expected).foreach(x =>
      assert(x._1 == x._2.copy(sessionId = x._1.sessionId))
    )
  }

  test("Verify build projection result for the single session per user") {
    val events = List(
      ClickEvent("1", "1", EventType.AppOpen.name, Timestamp.valueOf("2021-07-26 08:12:13"),
        Option(Map(EventAttribute.CampaignId -> "9", EventAttribute.ChannelId -> "Google Ads"))),
      ClickEvent("1", "2", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 10:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "333"))),
      ClickEvent("1", "3", EventType.Purchase.name, Timestamp.valueOf("2021-07-26 11:12:13"),
        Option(Map(EventAttribute.PurchaseId -> "444"))),
      ClickEvent("1", "4", EventType.AppClose.name, Timestamp.valueOf("2021-07-26 15:12:13"), None)
    )

    val purchases = List(
      Purchase("333", Timestamp.valueOf("2021-07-26 10:12:13"), 20.00, isConfirmed = true),
      Purchase("444", Timestamp.valueOf("2021-07-26 11:12:13"), 25.00, isConfirmed = false)
    )

    val eventsDS = spark.sparkContext.parallelize(events).toDS().as[ClickEvent]
    val purchasesDS = spark.sparkContext.parallelize(purchases).toDS().as[Purchase]

    val actual = projectionBuilder.build(eventsDS, purchasesDS).collect().toList.sortBy(_.purchaseId)

    val expected = List(
      DetailedPurchase("333", Timestamp.valueOf("2021-07-26 10:12:13"), Option(20.00), isConfirmed = true,
        "", "9", "Google Ads"),
      DetailedPurchase("444", Timestamp.valueOf("2021-07-26 11:12:13"), Option(25.00), isConfirmed = false,
        "", "9", "Google Ads")
    )

    actual.zip(expected).foreach(x =>
      assert(x._1 == x._2.copy(sessionId = x._1.sessionId))
    )
  }
}
