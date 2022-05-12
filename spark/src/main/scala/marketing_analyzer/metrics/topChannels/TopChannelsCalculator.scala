package marketing_analyzer.metrics.topChannels

import marketing_analyzer.model.DetailedPurchase
import org.apache.spark.sql.Dataset

trait TopChannelsCalculator {

  def calculate(purchasesDS: Dataset[DetailedPurchase]): Dataset[(String, String)]
}
