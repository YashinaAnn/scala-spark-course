package marketing_analyzer.integration

import marketing_analyzer.Spark.spark.implicits._
import marketing_analyzer.metrics.topCampaigns.{TopCampaignsDsCalculator, TopCampaignsSqlCalculator}
import marketing_analyzer.metrics.topChannels.{TopChannelsDsCalculator, TopChannelsSqlCalculator}
import marketing_analyzer.projection.PurchaseProjectionDsBuilder
import marketing_analyzer.utils.DataReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.sorted
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.reflect.io.File

class MarketingAnalyzerTest extends AnyFunSuite {

  private val clickStreamPath = "capstone-dataset/mobile_app_clickstream"
  private val purchasesPath = "capstone-dataset/user_purchases"

  private val projectionPath_DS = "output/projection"
  private val topCampaignsPath_DS = "output/ds/top_campaigns"
  private val topChannelsPath_DS = "output/ds/top_channels"

  private val projectionPath_UDAF = "output/projection_UDAF"
  private val topCampaignsPath_Sql = "output/sql/top_campaigns"
  private val topChannelsPath_Sql = "output/sql/top_channels"

  def deleteIfExists(path: String): Unit = {
    val file = File(path)
    if (file.exists) file.delete()
  }

  test("Verify projection build successfully using Datasets API") {
    deleteIfExists(projectionPath_DS)
    PurchaseProjectionDsBuilder.buildPurchaseProjection(clickStreamPath, purchasesPath, projectionPath_DS)
    assert(File(projectionPath_DS).exists)
  }

  test("Verify projection build successfully using UDAF") {
    deleteIfExists(projectionPath_UDAF)
    PurchaseProjectionDsBuilder.buildPurchaseProjection(clickStreamPath, purchasesPath, projectionPath_UDAF)
    assert(File(projectionPath_UDAF).exists)
  }

  test("Verify top campaigns metric calculated successfully using Datasets API") {
    deleteIfExists(topCampaignsPath_DS)
    val topLimit = 10

    TopCampaignsDsCalculator.calculateMetric(topLimit, projectionPath_DS, topCampaignsPath_DS)
    assert(File(topCampaignsPath_DS).exists)

    val topCampaigns = DataReader.readParquetDF(topCampaignsPath_DS).as[(String, Double)].collect()
    assert(topLimit == topCampaigns.length)
    topCampaigns.map(-_._2) shouldBe sorted
  }

  test("Verify top channels metric calculated successfully using Datasets API") {
    deleteIfExists(topChannelsPath_DS)

    TopChannelsDsCalculator.calculateMetric(projectionPath_DS, topChannelsPath_DS)
    assert(File(topChannelsPath_DS).exists)

    val topChannelsDS = DataReader.readParquetDF(topChannelsPath_DS).as[(String, String)].cache()
    assert(topChannelsDS.map(_._1).count() >= topChannelsDS.map(_._1).distinct().count())
  }

  test("Verify top campaigns metric calculated successfully using SQL") {
    deleteIfExists(topCampaignsPath_Sql)

    val topLimit = 10

    TopCampaignsSqlCalculator.calculateMetric(topLimit, projectionPath_UDAF, topCampaignsPath_Sql)
    assert(File(topCampaignsPath_Sql).exists)

    val topCampaigns = DataReader.readParquetDF(topCampaignsPath_Sql).as[(String, Double)].collect()
    assert(topLimit == topCampaigns.length)
    topCampaigns.map(-_._2) shouldBe sorted
  }

  test("Verify top channels metric calculated successfully using SQL") {
    deleteIfExists(topChannelsPath_Sql)

    TopChannelsSqlCalculator.calculateMetric(projectionPath_UDAF, topChannelsPath_Sql)
    assert(File(topChannelsPath_Sql).exists)

    val topChannelsDS = DataReader.readParquetDF(topChannelsPath_Sql).as[(String, String)].cache()
    assert(topChannelsDS.map(_._1).count() >= topChannelsDS.map(_._1).distinct().count())
  }

  test("Verify top campaigns metric results are identical for DS and SQL") {
    assert(File(topCampaignsPath_DS).exists)
    assert(File(topCampaignsPath_Sql).exists)
    val topCampaignsDS = DataReader.readParquetDF(topCampaignsPath_DS).as[(String, Double)].collect()
    val topCampaignsSql = DataReader.readParquetDF(topCampaignsPath_Sql).as[(String, Double)].collect()
    assert(topCampaignsDS.map(_._1) sameElements topCampaignsSql.map(_._1))
  }
}
