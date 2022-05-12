package marketing_analyzer.metrics.topChannles

import marketing_analyzer.metrics.topChannels.{TopChannelsDsCalculator, TopChannelsCalculator}

class TopChannelsTest extends AbstractTopChannelsTest {

  override protected val calculator: TopChannelsCalculator = TopChannelsDsCalculator
}
