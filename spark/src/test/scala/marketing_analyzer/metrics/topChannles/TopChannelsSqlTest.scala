package marketing_analyzer.metrics.topChannles

import marketing_analyzer.metrics.topChannels.{TopChannelsSqlCalculator, TopChannelsCalculator}

class TopChannelsSqlTest extends AbstractTopChannelsTest {

  override protected val calculator: TopChannelsCalculator = TopChannelsSqlCalculator
}
