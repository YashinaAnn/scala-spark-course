package marketing_analyzer.projection

import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model.Preamble._
import marketing_analyzer.model._
import org.apache.spark.sql.Dataset

object PurchaseProjectionDsBuilder extends AbstractPurchaseProjectionBuilder {

  override def build(clicksDS: Dataset[ClickEvent],
                     purchasesDS: Dataset[Purchase]): Dataset[DetailedPurchase] = {
    val attributesDS = extractAttributes(clicksDS)
    attributesDS.join(purchasesDS,
      attributesDS("id") === purchasesDS("purchaseId")
    ).as[DetailedPurchase]
  }

  def extractAttributes(clicksDS: Dataset[ClickEvent]): Dataset[PurchaseAttributes] = {
    clicksDS.filter {
      (event: ClickEvent) =>
        event.eventType == EventType.AppOpen.name || event.eventType == EventType.Purchase.name
    }
      .groupByKey(_.userId)
      .flatMapGroups((_, clickEvents) => getUserPurchaseAttributes(clickEvents.toList))
  }

  def getUserPurchaseAttributes(events: List[ClickEvent]): List[PurchaseAttributes] = {
    var sessionId = ""
    events
      .sortBy(_.eventTime)
      .map(event => {
        if (EventType.AppOpen.name == event.eventType)
          sessionId = PurchaseAttributes.generateSessionId
        (sessionId, event)
      })
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .flatMap {
        case (sessionId, events) => getSessionPurchaseAttributes(sessionId, events)
      }
      .toList
  }

  def getSessionPurchaseAttributes(sessionId: String,
                                   events: List[ClickEvent]): List[PurchaseAttributes] = events match {
    case openEvent :: purchaseEvents =>
      val openAttrs = openEvent.attributes.getOrElse(Map())
      purchaseEvents.map(event => {
        val purchaseAttrs = event.attributes.getOrElse(Map())
        PurchaseAttributes(
          id = purchaseAttrs.get(EventAttribute.PurchaseId).orNull,
          sessionId = sessionId,
          campaignId = openAttrs.get(EventAttribute.CampaignId).orNull,
          channelId = openAttrs.get(EventAttribute.ChannelId).orNull)
      })
  }
}
