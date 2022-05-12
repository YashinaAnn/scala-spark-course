package marketing_analyzer.model

import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.UUID

case class Purchase(purchaseId: String, purchaseTime: Timestamp,
                    billingCost: Double, isConfirmed: Boolean)

object Purchase {

  val schema: StructType = StructType(Array(
    StructField("purchaseId", StringType, nullable = false),
    StructField("purchaseTime", StringType, nullable = false),
    StructField("billingCost", DoubleType, nullable = false),
    StructField("isConfirmed", BooleanType, nullable = true)
  ))
}


case class DetailedPurchase(purchaseId: String, purchaseTime: Timestamp,
                            billingCost: Option[Double], isConfirmed: Boolean,
                            sessionId: String, campaignId: String,
                            channelId: String)


case class PurchaseAttributes(id: String, sessionId: String,
                              campaignId: String, channelId: String)

object PurchaseAttributes {

  def generateSessionId: String = UUID.randomUUID().toString
}