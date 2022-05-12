package marketing_analyzer.projection

import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.model._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, TypedColumn}

object PurchaseProjectionUDAFBuilder extends AbstractPurchaseProjectionBuilder {

  override def build(clicksDS: Dataset[ClickEvent],
                     purchaseDS: Dataset[Purchase]): Dataset[DetailedPurchase] = {
    val attributesDS = clicksDS
      .filter {
        (event: ClickEvent) =>
          event.eventType == EventType.AppOpen.name || event.eventType == EventType.Purchase.name
      }
      .groupByKey(_.userId)
      .agg(aggregator)
      .map {
        case (_, attributes) => attributes
      }

    attributesDS.join(purchaseDS,
      attributesDS("id") === purchaseDS("purchaseId")
    ).as[DetailedPurchase]
  }

  val aggregator: TypedColumn[ClickEvent, PurchaseAttributes] =
    new Aggregator[ClickEvent, Map[String, String], PurchaseAttributes] {

      override def zero: Map[String, String] = Map()

      override def reduce(attrs: Map[String, String], event: ClickEvent): Map[String, String] = {
        attrs ++ event.attributes.getOrElse(Map())
      }

      override def merge(attr1: Map[String, String], attr2: Map[String, String]): Map[String, String] = {
        attr1 ++ attr2
      }

      override def finish(attributes: Map[String, String]): PurchaseAttributes = {
        PurchaseAttributes(
          id = attributes.get(EventAttribute.PurchaseId).orNull,
          sessionId = PurchaseAttributes.generateSessionId,
          campaignId = attributes.get(EventAttribute.CampaignId).orNull,
          channelId = attributes.get(EventAttribute.ChannelId).orNull)
      }

      override def bufferEncoder: Encoder[Map[String, String]] = implicitly[Encoder[Map[String, String]]]

      override def outputEncoder: Encoder[PurchaseAttributes] = implicitly[Encoder[PurchaseAttributes]]

    }.toColumn
}
