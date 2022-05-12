package marketing_analyzer.model

object EventAttribute {
  val PurchaseId = "purchase_id"
  val CampaignId = "campaign_id"
  val ChannelId = "channel_id"
}

object EventType extends Enumeration {
  protected case class EventTypeVal(name: String) extends super.Val
  val AppOpen: EventTypeVal = EventTypeVal("app_open")
  val SearchProduct: EventTypeVal = EventTypeVal("search_product")
  val ViewProductDetails: EventTypeVal = EventTypeVal("view_product_details")
  val Purchase: EventTypeVal = EventTypeVal("purchase")
  val AppClose: EventTypeVal = EventTypeVal("app_close")
}
