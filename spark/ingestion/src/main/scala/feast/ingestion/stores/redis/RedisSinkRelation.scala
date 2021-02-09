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

import java.util

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.redislabs.provider.redis.util.PipelineUtils.{foreachWithPipeline, mapWithPipeline}
import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig, RedisEndpoint, RedisNode}
import feast.ingestion.utils.TypeConversion
import feast.proto.storage.RedisProto.RedisKeyV2
import feast.proto.types.ValueProto
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.RedisSinkMetricSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.util.JedisClusterCRC16

import scala.collection.JavaConverters._

/**
  * High-level writer to Redis. Relies on `Persistence` implementation for actual storage layout.
  * Here we define general flow:
  *
  * 1. Deduplicate rows within one batch (group by key and get only latest (by timestamp))
  * 2. Read last-stored timestamp from Redis
  * 3. Check if current timestamp is more recent than already saved one
  * 4. Save to storage if it's the case
  */
class RedisSinkRelation(override val sqlContext: SQLContext, config: SparkRedisConfig)
    extends BaseRelation
    with InsertableRelation
    with Serializable {

  import RedisSinkRelation._

  private implicit val redisConfig: RedisConfig = {
    new RedisConfig(
      new RedisEndpoint(sqlContext.sparkContext.getConf)
    )
  }

  private implicit val readWriteConfig: ReadWriteConfig = {
    ReadWriteConfig.fromSparkConf(sqlContext.sparkContext.getConf)
  }

  override def schema: StructType = ???

  val MAX_EXPIRED_TIMESTAMP = new java.sql.Timestamp(Timestamps.MAX_VALUE.getSeconds * 1000)

  val persistence: Persistence = new HashTypePersistence(config)

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // repartition for deduplication
    val dataToStore =
      if (config.repartitionByEntity && data.rdd.getNumPartitions > 1)
        data
          .repartition(data.rdd.getNumPartitions, config.entityColumns.map(col): _*)
          .localCheckpoint()
      else data

    dataToStore.foreachPartition { partition: Iterator[Row] =>
      // grouped iterator to only allocate memory for a portion of rows
      partition.grouped(config.iteratorGroupingSize).foreach { batch =>
        // group by key and keep only latest row per each key
        val rowsWithKey: Map[RedisKeyV2, Row] =
          compactRowsToLatestTimestamp(batch.map(row => dataKeyId(row) -> row)).toMap

        groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
          val conn = node.connect()
          // retrieve latest stored values
          val storedValues = mapWithPipeline(conn, keys) { (pipeline, key) =>
            persistence.get(pipeline, key.toByteArray)
          }.map(_.asInstanceOf[util.Map[Array[Byte], Array[Byte]]])

          val timestamps     = storedValues.map(persistence.storedTimestamp)
          val timestampByKey = keys.zip(timestamps).toMap

          val expiryTimestampByKey = keys
            .zip(storedValues)
            .map { case (key, storedValue) =>
              (key, newExpiryTimestamp(rowsWithKey(key), storedValue))
            }
            .toMap

          foreachWithPipeline(conn, keys) { (pipeline, key) =>
            val row = rowsWithKey(key)

            timestampByKey(key) match {
              case Some(t) if (t.after(row.getAs[java.sql.Timestamp](config.timestampColumn))) =>
                ()
              case _ =>
                if (metricSource.nonEmpty) {
                  val lag = System.currentTimeMillis() - row
                    .getAs[java.sql.Timestamp](config.timestampColumn)
                    .getTime

                  metricSource.get.METRIC_TOTAL_ROWS_INSERTED.inc()
                  metricSource.get.METRIC_ROWS_LAG.update(lag)
                }
                persistence.save(
                  pipeline,
                  key.toByteArray,
                  row,
                  expiryTimestampByKey(key),
                  MAX_EXPIRED_TIMESTAMP
                )
            }
          }
          conn.close()
        }
      }
    }
  }

  private def compactRowsToLatestTimestamp(rows: Seq[(RedisKeyV2, Row)]) = rows
    .groupBy(_._1)
    .values
    .map(_.maxBy(_._2.getAs[java.sql.Timestamp](config.timestampColumn).getTime))

  /**
    * Key is built from entities columns values with prefix of entities columns names.
    */
  private def dataKeyId(row: Row): RedisKeyV2 = {
    val types = row.schema.fields.map(f => (f.name, f.dataType)).toMap

    val sortedEntities = config.entityColumns.sorted.toSeq
    val entityValues = sortedEntities
      .map(entity => (row.getAs[Any](entity), types(entity)))
      .map { case (value, v_type) =>
        TypeConversion.sqlTypeToProtoValue(value, v_type).asInstanceOf[ValueProto.Value]
      }

    RedisKeyV2
      .newBuilder()
      .setProject(config.projectName)
      .addAllEntityNames(sortedEntities.asJava)
      .addAllEntityValues(entityValues.asJava)
      .build
  }

  private lazy val metricSource: Option[RedisSinkMetricSource] = {
    MetricInitializationLock.synchronized {
      // RedisSinkMetricSource needs to be registered on executor and SparkEnv must already exist.
      // Which is problematic, since metrics system is initialized before SparkEnv set.
      // That's why I moved source registering here
      if (SparkEnv.get.metricsSystem.getSourcesByName(RedisSinkMetricSource.sourceName).isEmpty) {
        SparkEnv.get.metricsSystem.registerSource(new RedisSinkMetricSource)
      }
    }

    SparkEnv.get.metricsSystem.getSourcesByName(RedisSinkMetricSource.sourceName) match {
      case Seq(source: RedisSinkMetricSource) => Some(source)
      case _                                  => None
    }
  }

  private def groupKeysByNode(
      nodes: Array[RedisNode],
      keys: Iterator[RedisKeyV2]
  ): Iterator[(RedisNode, Array[RedisKeyV2])] = {
    keys
      .map(key => (getMasterNode(nodes, key), key))
      .toArray
      .groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2)))
      .iterator
  }

  private def getMasterNode(nodes: Array[RedisNode], key: RedisKeyV2): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key.toByteArray)

    nodes.filter { node => node.startSlot <= slot && node.endSlot >= slot }.filter(_.idx == 0)(0)
  }

  private def newExpiryTimestamp(
      row: Row,
      value: util.Map[Array[Byte], Array[Byte]]
  ): java.sql.Timestamp = {
    val maxExpiryOtherFeatureTables: Long = value.asScala.toMap
      .map { case (key, value) =>
        (key.map(_.toChar).mkString, value)
      }
      .filterKeys(_.startsWith(config.expiryPrefix))
      .filterKeys(_.split(":").last != config.namespace)
      .values
      .map(value => Timestamp.parseFrom(value).getSeconds * 1000)
      .reduceOption(_ max _)
      .getOrElse(0)

    val rowExpiry: Long =
      if (config.maxAge > 0)
        (row
          .getAs[java.sql.Timestamp](config.timestampColumn)
          .getTime + config.maxAge * 1000)
      else MAX_EXPIRED_TIMESTAMP.getTime

    val maxExpiry = maxExpiryOtherFeatureTables max rowExpiry
    new java.sql.Timestamp(maxExpiry)

  }
}

object RedisSinkRelation {
  object MetricInitializationLock
}
