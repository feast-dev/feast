/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.stores.redis

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import redis.clients.jedis.{Pipeline, Response}

import scala.jdk.CollectionConverters._
import com.google.protobuf.Timestamp
import feast.ingestion.utils.TypeConversion
import scala.util.hashing.MurmurHash3

/**
  * Use Redis hash type as storage layout. Every feature is stored as separate entry in Hash.
  * Also additional `timestamp` column is stored per FeatureTable to track update time.
  *
  * Keys are hashed as murmur3(`featureTableName` : `featureName`).
  * Values are serialized with protobuf (`ValueProto`).
  */
class HashTypePersistence(config: SparkRedisConfig) extends Persistence with Serializable {
  def encodeRow(
      keyColumns: Array[String],
      timestampField: String,
      value: Row
  ): Map[Array[Byte], Array[Byte]] = {
    val fields = value.schema.fields.map(_.name)
    val types  = value.schema.fields.map(f => (f.name, f.dataType)).toMap
    val kvMap  = value.getValuesMap[Any](fields)

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
        encodeKey(k) -> encodeValue(v, types(k))
      }

    val timestamp = Seq(
      (
        timestampField.getBytes,
        encodeValue(value.getAs[Timestamp](config.timestampColumn), TimestampType)
      )
    )

    values ++ timestamp
  }

  def encodeValue(value: Any, `type`: DataType): Array[Byte] = {
    TypeConversion.sqlTypeToProtoValue(value, `type`).toByteArray
  }

  def encodeKey(key: String): Array[Byte] = {
    val fullFeatureReference = s"${config.namespace}:$key"
    MurmurHash3.stringHash(fullFeatureReference).toHexString.getBytes
  }

  def save(
      pipeline: Pipeline,
      key: Array[Byte],
      value: Map[Array[Byte], Array[Byte]],
      ttl: Int
  ): Unit = {
    pipeline.hset(key, value.asJava)
    if (ttl > 0) {
      pipeline.expire(key, ttl)
    }
  }

  def getTimestamp(
      pipeline: Pipeline,
      key: Array[Byte],
      timestampField: String
  ): Response[Array[Byte]] = {
    pipeline.hget(key, timestampField.getBytes)
  }

}
