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

import collection.JavaConverters._
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.joda.time.{DateTime, Seconds}
import org.scalacheck._
import org.scalatest._
import redis.clients.jedis.Jedis
import feast.ingestion.helpers.RedisStorageHelper._
import feast.ingestion.helpers.DataHelper._
import feast.proto.storage.RedisProto.RedisKeyV2
import feast.proto.types.ValueProto

case class Row(customer: String, feature1: Int, feature2: Float, eventTimestamp: java.sql.Timestamp)

class BatchPipelineIT extends SparkSpec with ForAllTestContainer {

  override val container = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.redis.host", container.host)
    .set("spark.redis.port", container.mappedPort(6379).toString)

  trait Scope {
    val jedis = new Jedis("localhost", container.mappedPort(6379))
    jedis.flushAll()

    def rowGenerator(start: DateTime, end: DateTime, customerGen: Option[Gen[String]] = None) =
      for {
        customer <- customerGen.getOrElse(Gen.asciiPrintableStr)
        feature1 <- Gen.choose(0, 100)
        feature2 <- Gen.choose[Float](0, 1)
        eventTimestamp <- Gen
          .choose(0, Seconds.secondsBetween(start, end).getSeconds)
          .map(start.withMillisOfSecond(0).plusSeconds)
      } yield Row(customer, feature1, feature2, new java.sql.Timestamp(eventTimestamp.getMillis))

    def encodeEntityKey(row: Row, featureTable: FeatureTable): Array[Byte] = {
      RedisKeyV2
        .newBuilder()
        .setProject(featureTable.project)
        .addAllEntityNames(featureTable.entities.map(_.name).sorted.asJava)
        .addEntityValues(ValueProto.Value.newBuilder().setStringVal(row.customer))
        .build
        .toByteArray
    }

    def groupByEntity(row: Row) =
      new String(encodeEntityKey(row, config.featureTable))

    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        project = "default",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT)
        )
      ),
      startTime = DateTime.parse("2020-08-01"),
      endTime = DateTime.parse("2020-09-01")
    )
  }

  "Parquet source file" should "be ingested in redis" in new Scope {
    val gen      = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows     = generateDistinctRows(gen, 10000, groupByEntity)
    val tempPath = storeAsParquet(sparkSession, rows)
    val configWithOfflineSource = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp")
    )

    BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    rows.foreach(r => {
      val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs"                 -> r.eventTimestamp
        )
      )
    })
  }

  "Ingested rows" should "be compacted before storing by timestamp column" in new Scope {
    val entities = (0 to 10000).map(_.toString)

    val genLatest = rowGenerator(
      DateTime.parse("2020-08-15"),
      DateTime.parse("2020-09-01"),
      Some(Gen.oneOf(entities))
    )
    val latest = generateDistinctRows(genLatest, 10000, groupByEntity)

    val genOld = rowGenerator(
      DateTime.parse("2020-08-01"),
      DateTime.parse("2020-08-14"),
      Some(Gen.oneOf(entities))
    )
    val old = generateDistinctRows(genOld, 10000, groupByEntity)

    val tempPath = storeAsParquet(sparkSession, latest ++ old)
    val configWithOfflineSource =
      config.copy(source = FileSource(tempPath, Map.empty, "eventTimestamp"))

    BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    latest.foreach(r => {
      val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs"                 -> r.eventTimestamp
        )
      )
    })
  }

  "Old rows in ingestion" should "not overwrite more recent rows from storage" in new Scope {
    val entities = (0 to 10000).map(_.toString)

    val genLatest = rowGenerator(
      DateTime.parse("2020-08-15"),
      DateTime.parse("2020-09-01"),
      Some(Gen.oneOf(entities))
    )
    val latest = generateDistinctRows(genLatest, 10000, groupByEntity)

    val tempPath1 = storeAsParquet(sparkSession, latest)
    val config1   = config.copy(source = FileSource(tempPath1, Map.empty, "eventTimestamp"))

    BatchPipeline.createPipeline(sparkSession, config1)

    val genOld = rowGenerator(
      DateTime.parse("2020-08-01"),
      DateTime.parse("2020-08-14"),
      Some(Gen.oneOf(entities))
    )
    val old = generateDistinctRows(genOld, 10000, groupByEntity)

    val tempPath2 = storeAsParquet(sparkSession, old)
    val config2   = config.copy(source = FileSource(tempPath2, Map.empty, "eventTimestamp"))

    BatchPipeline.createPipeline(sparkSession, config2)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    latest.foreach(r => {
      val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs"                 -> r.eventTimestamp
        )
      )
    })
  }

  "Invalid rows" should "not be ingested and stored to deadletter instead" in new Scope {
    val gen  = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 100, groupByEntity)

    val rowsWithNullEntity = rows.map(_.copy(customer = null))

    val tempPath = storeAsParquet(sparkSession, rowsWithNullEntity)
    val deadletterConfig = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp"),
      deadLetterPath = Some(generateTempPath("deadletters"))
    )

    BatchPipeline.createPipeline(sparkSession, deadletterConfig)

    jedis.keys("*").toArray should be(empty)

    sparkSession.read
      .parquet(deadletterConfig.deadLetterPath.get)
      .count() should be(rows.length)
  }

  "Columns from source" should "be mapped according to configuration" in new Scope {
    val gen  = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 100, groupByEntity)

    val tempPath = storeAsParquet(sparkSession, rows)

    val configWithMapping = config.copy(
      featureTable = config.featureTable.copy(
        entities = Seq(Field("entity", ValueType.Enum.STRING)),
        features = Seq(
          Field("new_feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT)
        )
      ),
      source = FileSource(
        tempPath,
        Map(
          "entity"       -> "customer",
          "new_feature1" -> "feature1"
        ),
        "eventTimestamp"
      )
    )

    BatchPipeline.createPipeline(sparkSession, configWithMapping)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    rows.foreach(r => {
      val storedValues =
        jedis.hgetAll(encodeEntityKey(r, configWithMapping.featureTable)).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("new_feature1") -> r.feature1,
          featureKeyEncoder("feature2")     -> r.feature2,
          "_ts:test-fs"                     -> r.eventTimestamp
        )
      )
    })
  }
}
