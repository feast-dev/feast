package feast.ingestion

import feast.ingestion.Modes.Modes
import org.joda.time.DateTime

object Modes extends Enumeration {
  type Modes = Value
  val Offline, Online = Value
}

abstract class StoreConfig

case class RedisConfig(connection: String) extends StoreConfig

abstract class MetricConfig

case class StatsDConfig(host: String, port: Int) extends MetricConfig

case class IngestionJobConfig(
                               mode: Modes = Modes.Offline,
                               featureTable: FeatureTable = null,
                               startTime: DateTime = DateTime.now(),
                               endTime: DateTime = DateTime.now(),
                               store: StoreConfig = RedisConfig("localhost:6379"),
                               metrics: Option[MetricConfig] = Some(StatsDConfig("localhost", 9125)),
                               deadLetterPath: Option[String] = None
                             )
