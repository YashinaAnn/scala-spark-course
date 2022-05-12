package marketing_analyzer.projection

import marketing_analyzer.model.{ClickEvent, DetailedPurchase, Purchase}
import org.apache.spark.sql.Dataset

trait PurchaseProjectionBuilder {

  def build(clicksDS: Dataset[ClickEvent], purchasesDS: Dataset[Purchase]): Dataset[DetailedPurchase]
}
