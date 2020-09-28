package feast.ingestion

abstract class Source {
  def mapping: Map[String, String]
  def timestampColumn: String
}

abstract class OfflineSource extends Source
abstract class OnlineSource extends Source

case class GSSource(path: String, override val mapping: Map[String, String], override val  timestampColumn: String) extends OfflineSource
case class BQSource(project: String, dataset: String, table: String, override val mapping: Map[String, String], override val  timestampColumn: String) extends OfflineSource


case class KafkaSource(bootstrapServers: String, topic: String, override val mapping: Map[String, String], override val  timestampColumn: String) extends OnlineSource

case class Field(name: String, `type`: feast.proto.types.ValueProto.ValueType.Enum)

case class FeatureTable(name: String,
                        entities: Seq[Field],
                        features: Seq[Field],
                        offline_source: Option[OfflineSource] = None,
                        online_source: Option[OnlineSource] = None)
