package marketing_analyzer.model

import org.apache.spark.sql.{Encoder, Encoders}

import java.sql.Timestamp
import scala.reflect.ClassTag

object Preamble {

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] = Encoders.kryo[A](ct)

  implicit def ordered: Ordering[Timestamp] = (x: Timestamp, y: Timestamp) => x compareTo y
}