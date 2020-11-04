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
package feast.ingestion

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.StreamingQuery

trait BasePipeline {
  def createSparkSession(jobConfig: IngestionJobConfig): SparkSession = {
    // workaround for issue with arrow & netty
    // see https://github.com/apache/arrow/tree/master/java#java-properties
    System.setProperty("io.netty.tryReflectionSetAccessible", "true")

    val conf = new SparkConf()

    jobConfig.store match {
      case RedisConfig(host, port, ssl) =>
        conf
          .set("spark.redis.host", host)
          .set("spark.redis.port", port.toString)
          .set("spark.redis.ssl", ssl.toString)
    }

    jobConfig.metrics match {
      case Some(c: StatsDConfig) =>
        conf
          .set(
            "spark.metrics.conf.*.source.redis.class",
            "org.apache.spark.metrics.source.RedisSinkMetricSource"
          )
          .set(
            "spark.metrics.conf.*.source.redis.labels",
            s"feature_table=${jobConfig.featureTable.name}"
          )
          .set(
            "spark.metrics.conf.*.sink.statsd.class",
            "org.apache.spark.metrics.sink.StatsdSinkWithTags"
          )
          .set("spark.metrics.conf.*.sink.statsd.host", c.host)
          .set("spark.metrics.conf.*.sink.statsd.port", c.port.toString)
          .set("spark.metrics.conf.*.sink.statsd.period", "30")
          .set("spark.metrics.conf.*.sink.statsd.unit", "seconds")
          .set("spark.metrics.namespace", jobConfig.mode.toString)
      case None => ()
    }

    jobConfig.stencilURL match {
      case Some(url: String) =>
        conf
          .set("feast.ingestion.registry.proto.kind", "local")
          .set("feast.ingestion.registry.proto.url", url)
      case None => ()
    }

    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  def createPipeline(sparkSession: SparkSession, config: IngestionJobConfig): Option[StreamingQuery]

  /**
    * Build column projection using custom mapping with fallback to feature|entity names.
    */
  def inputProjection(
      source: Source,
      features: Seq[Field],
      entities: Seq[Field]
  ): Array[Column] = {
    val featureColumns = features
      .filter(f => !source.fieldMapping.contains(f.name))
      .map(f => (f.name, f.name)) ++ source.fieldMapping

    val timestampColumn = Seq((source.eventTimestampColumn, source.eventTimestampColumn))
    val entitiesColumns =
      entities
        .filter(e => !source.fieldMapping.contains(e.name))
        .map(e => (e.name, e.name))

    (featureColumns ++ entitiesColumns ++ timestampColumn).map { case (alias, source) =>
      expr(source).alias(alias)
    }.toArray
  }
}
