package marketing_analyzer.metrics.topCampaigns

import marketing_analyzer.model.DetailedPurchase
import org.apache.spark.sql.Dataset

trait TopCampaignsCalculator {

  def calculate(n: Int, purchasesDS: Dataset[DetailedPurchase]): Dataset[(String, Double)]
}
