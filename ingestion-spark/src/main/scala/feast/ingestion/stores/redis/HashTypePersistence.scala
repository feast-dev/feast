package feast.ingestion.stores.redis


import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import redis.clients.jedis.{Pipeline, Response}

import scala.jdk.CollectionConverters._
import com.google.protobuf.Timestamp
import feast.ingestion.utils.TypeConversion
import feast.proto.types.ValueProto


class HashTypePersistence(config: SparkRedisConfig) extends Serializable {
  def encodeRow(keyColumns: Array[String], timestampField: String, value: Row): Map[Array[Byte], Array[Byte]] = {
    val fields = value.schema.fields.map(_.name)
    val types = value.schema.fields.map(f => (f.name, f.dataType)).toMap
    val kvMap = value.getValuesMap[Any](fields)

    val values = kvMap
      .filter { case (_, v) =>
        // don't store null values
        v != null
      }
      .filter { case (k, _) =>
        // don't store entities & timestamp
        !keyColumns.contains(k) && k != config.timestampColumn
      }
      .map { case (k, v) =>
        k.getBytes -> encodeValue(v, types(k))
      }

    val timestamp = Seq((
      timestampField.getBytes,
      encodeValue(value.getAs[Timestamp](config.timestampColumn), TimestampType)))

    values ++ timestamp
  }

  def encodeValue(value: Any, `type`: DataType): Array[Byte] = {
    TypeConversion.sqlTypeToProtoValue(value, `type`).toByteArray
  }

  def save(pipeline: Pipeline, key: String, value: Map[Array[Byte], Array[Byte]], ttl: Int): Unit = {
    pipeline.hset(key.getBytes, value.asJava)
    if (ttl > 0) {
      pipeline.expire(key, ttl)
    }
  }

  def getTimestamp(pipeline: Pipeline, key: String, timestampField: String): Response[Array[Byte]] = {
    pipeline.hget(key.getBytes(), timestampField.getBytes)
  }

}
