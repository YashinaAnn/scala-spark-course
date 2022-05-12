package marketing_analyzer.config

import pureconfig.ConfigSource
import pureconfig.generic.auto._

case class Configuration(clickstreamPath: String,
                         purchasesPath: String,
                         projectionPath: String,
                         topCampaignsPath: String,
                         topChannelsPath: String,
                         topCampaignsLimit: Int)

object Configuration {
  val config: Configuration = ConfigSource.default.load[Configuration] match {
    case Right(conf) => conf
    case Left(error) => throw new Exception(error.toString)
  }
}