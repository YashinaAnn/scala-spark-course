package marketing_analyzer.model

import marketing_analyzer.utils.StringUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.sql.Timestamp

case class ClickEventInput(userId: String, eventId: String,
                           eventTime: Timestamp, eventType: String,
                           attributes: String)

object ClickEventInput {

  val schema: StructType = StructType(Array(
    StructField("userId", StringType, nullable = false),
    StructField("eventId", StringType, nullable = false),
    StructField("eventType", StringType, nullable = false),
    StructField("eventTime", StringType, nullable = false),
    StructField("attributes", StringType, nullable = true)
  ))
}


case class ClickEvent(userId: String, eventId: String,
                      eventType: String, eventTime: Timestamp,
                      attributes: Option[Map[String, String]])

object ClickEvent {

  def fromClickEventInput(event: ClickEventInput): ClickEvent = {
    ClickEvent(event.userId, event.eventId,
      event.eventType, event.eventTime,
      parseAttributesMap(event.attributes))
  }

  def parseAttributesMap(map: String): Option[Map[String, String]] = map match {
    case null => None
    case _ => Option(StringUtil.parseStr2Map(map))
  }
}