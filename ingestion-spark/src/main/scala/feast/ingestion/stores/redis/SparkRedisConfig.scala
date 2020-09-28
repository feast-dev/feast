package feast.ingestion.stores.redis


case class SparkRedisConfig(
                             namespace: String,
                             entityNames: Array[String],
                             entityColumns: Array[String],
                             timestampColumn: String,
                             iteratorGroupingSize: Int = 1000,
                             timestampPrefix: String = "_ts",
                             repartitionByEntity: Boolean = true
                           )

object SparkRedisConfig {
  val NAMESPACE = "namespace"
  val ENTITY_NAMES = "entity_names"
  val ENTITY_COLUMNS = "entity_columns"
  val TS_COLUMN = "timestamp_column"
  val ENTITY_REPARTITION = "entity_repartition"

  def parse(parameters: Map[String, String]): SparkRedisConfig =
    SparkRedisConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      entityNames = parameters.getOrElse(ENTITY_NAMES, "").split(","),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp"),
      repartitionByEntity = parameters.getOrElse(ENTITY_REPARTITION, "true") == "true"
    )
}
