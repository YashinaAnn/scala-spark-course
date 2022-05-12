package marketing_analyzer.utils

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.util.Calendar

object TimeUtils {

  val calendar: Calendar = Calendar.getInstance

  def getTimestamp: Timestamp = Timestamp.from(calendar.toInstant)

  def plusMinutes(timestamp: Timestamp, minutes: Int) : Timestamp = {
    Timestamp.from(timestamp.toInstant.plus(minutes, ChronoUnit.MINUTES))
  }

  def minusMinutes(timestamp: Timestamp, minutes: Int) : Timestamp = {
    Timestamp.from(timestamp.toInstant.minus(minutes, ChronoUnit.MINUTES))
  }
}
