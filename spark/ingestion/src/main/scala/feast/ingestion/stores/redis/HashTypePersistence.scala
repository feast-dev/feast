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

import java.nio.charset.StandardCharsets
import java.util

import com.google.common.hash.Hashing
import com.google.protobuf.Timestamp
import feast.ingestion.utils.TypeConversion
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import redis.clients.jedis.{Pipeline, Response}

import scala.jdk.CollectionConverters._

/**
  * Use Redis hash type as storage layout. Every feature is stored as separate entry in Hash.
  * Also additional `timestamp` column is stored per FeatureTable to track update time.
  *
  * Keys are hashed as murmur3(`featureTableName` : `featureName`).
  * Values are serialized with protobuf (`ValueProto`).
  */
class HashTypePersistence(config: SparkRedisConfig) extends Persistence with Serializable {

  private def encodeRow(
      value: Row,
      maxExpiryTimestamp: java.sql.Timestamp
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
        !config.entityColumns.contains(k) && k != config.timestampColumn
      }
      .map { case (k, v) =>
        encodeKey(k) -> encodeValue(v, types(k))
      }

    val timestampHash = Seq(
      (
        timestampHashKey(config.namespace).getBytes,
        encodeValue(value.getAs[Timestamp](config.timestampColumn), TimestampType)
      )
    )

    val expiryUnixTimestamp = {
      if (config.maxAge > 0)
        value.getAs[java.sql.Timestamp](config.timestampColumn).getTime + config.maxAge * 1000
      else maxExpiryTimestamp.getTime
    }
    val expiryTimestamp = new java.sql.Timestamp(expiryUnixTimestamp)
    val expiryTimestampHash = Seq(
      (
        expiryTimestampHashKey(config.namespace).getBytes,
        encodeValue(expiryTimestamp, TimestampType)
      )
    )

    values ++ timestampHash ++ expiryTimestampHash
  }

  private def encodeValue(value: Any, `type`: DataType): Array[Byte] = {
    TypeConversion.sqlTypeToProtoValue(value, `type`).toByteArray
  }

  private def encodeKey(key: String): Array[Byte] = {
    val fullFeatureReference = s"${config.namespace}:$key"
    Hashing.murmur3_32.hashString(fullFeatureReference, StandardCharsets.UTF_8).asBytes()
  }

  private def timestampHashKey(namespace: String): String = {
    s"${config.timestampPrefix}:${namespace}"
  }

  private def expiryTimestampHashKey(namespace: String): String = {
    s"${config.expiryPrefix}:${namespace}"
  }

  private def decodeTimestamp(encodedTimestamp: Array[Byte]): java.sql.Timestamp = {
    new java.sql.Timestamp(Timestamp.parseFrom(encodedTimestamp).getSeconds * 1000)
  }

  override def save(
      pipeline: Pipeline,
      key: Array[Byte],
      row: Row,
      expiryTimestamp: java.sql.Timestamp,
      maxExpiryTimestamp: java.sql.Timestamp
  ): Unit = {
    val value = encodeRow(row, maxExpiryTimestamp).asJava
    pipeline.hset(key, value)
    if (expiryTimestamp.equals(maxExpiryTimestamp)) {
      pipeline.persist(key)
    } else {
      pipeline.expireAt(key, expiryTimestamp.getTime / 1000)
    }
  }

  override def get(
      pipeline: Pipeline,
      key: Array[Byte]
  ): Response[util.Map[Array[Byte], Array[Byte]]] = {
    pipeline.hgetAll(key)
  }

  override def storedTimestamp(
      value: util.Map[Array[Byte], Array[Byte]]
  ): Option[java.sql.Timestamp] = {
    value.asScala.toMap
      .map { case (key, value) =>
        (key.map(_.toChar).mkString, value)
      }
      .get(timestampHashKey(config.namespace))
      .map(value => decodeTimestamp(value))
  }
}
