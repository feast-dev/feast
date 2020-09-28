package feast.ingestion.stores.redis

import redis.clients.jedis.Protocol

case class SparkRedisConfig(
                             val namespace: String,
                             val entityNames: Array[String],
                             val entityColumns: Array[String],
                             val timestampColumn: String,
                             val iteratorGroupingSize: Int = 1000,
                             val timestampPrefix: String = "_ts"
                           )

object SparkRedisConfig {
  val NAMESPACE = "namespace"
  val ENTITY_NAMES = "entity_names"
  val ENTITY_COLUMNS = "entity_columns"
  val TS_COLUMN = "timestamp_column"

  def parse(parameters: Map[String, String]): SparkRedisConfig =
    SparkRedisConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      entityNames = parameters.getOrElse(ENTITY_NAMES, "").split(","),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp")
    )
}
