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

import java.nio.file.Paths
import java.sql.Timestamp

import collection.JavaConverters._
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.protobuf.util.Timestamps
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.{SparkConf, SparkEnv}
import org.joda.time.{DateTime, Seconds}
import org.scalacheck._
import org.scalatest._
import redis.clients.jedis.Jedis
import feast.ingestion.helpers.RedisStorageHelper._
import feast.ingestion.helpers.DataHelper._
import feast.ingestion.metrics.StatsDStub
import feast.proto.storage.RedisProto.RedisKeyV2
import feast.proto.types.ValueProto
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class TestRow(
    customer: String,
    feature1: Int,
    feature2: Float,
    eventTimestamp: java.sql.Timestamp
)

class BatchPipelineIT extends SparkSpec with ForAllTestContainer {

  override val container = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))
  val statsDStub         = new StatsDStub

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.redis.host", container.host)
    .set("spark.redis.port", container.mappedPort(6379).toString)
    .set("spark.metrics.conf.*.sink.statsd.port", statsDStub.port.toString)

  trait Scope {
    val jedis = new Jedis("localhost", container.mappedPort(6379))
    jedis.flushAll()

    statsDStub.receivedMetrics // clean the buffer

    implicit def testRowEncoder: Encoder[TestRow] = ExpressionEncoder()

    def rowGenerator(start: DateTime, end: DateTime, customerGen: Option[Gen[String]] = None) =
      for {
        customer <- customerGen.getOrElse(Gen.asciiPrintableStr)
        feature1 <- Gen.choose(0, 100)
        feature2 <- Gen.choose[Float](0, 1)
        eventTimestamp <- Gen
          .choose(0, Seconds.secondsBetween(start, end).getSeconds - 1)
          .map(start.withMillisOfSecond(0).plusSeconds)
      } yield TestRow(
        customer,
        feature1,
        feature2,
        new java.sql.Timestamp(eventTimestamp.getMillis)
      )

    def encodeEntityKey(row: TestRow, featureTable: FeatureTable): Array[Byte] = {
      RedisKeyV2
        .newBuilder()
        .setProject(featureTable.project)
        .addAllEntityNames(featureTable.entities.map(_.name).sorted.asJava)
        .addEntityValues(ValueProto.Value.newBuilder().setStringVal(row.customer))
        .build
        .toByteArray
    }

    def groupByEntity(row: TestRow) =
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
      endTime = DateTime.parse("2020-09-01"),
      metrics = Some(StatsDConfig(host = "localhost", port = statsDStub.port))
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
      val encodedEntityKey = encodeEntityKey(r, config.featureTable)
      val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs"                 -> r.eventTimestamp,
          "_ex:test-fs"                 -> new Timestamp(Timestamps.MAX_VALUE.getSeconds * 1000)
        )
      )
      val keyTTL = jedis.ttl(encodedEntityKey).toInt
      keyTTL shouldEqual -1

    })

    SparkEnv.get.metricsSystem.report()
    statsDStub.receivedMetrics should contain.allElementsOf(
      Map(
        "driver.ingestion_pipeline.read_from_source_count" -> rows.length,
        "driver.redis_sink.feature_row_ingested_count"     -> rows.length
      )
    )
  }

  "Parquet source file" should "be ingested in redis with expiry time equal to the largest of (event_timestamp + max_age) for" +
    "all feature tables associated with the entity" in new Scope {
      val startDate = new DateTime().minusDays(1).withTimeAtStartOfDay()
      val endDate   = new DateTime().withTimeAtStartOfDay()
      val gen       = rowGenerator(startDate, endDate)
      val rows      = generateDistinctRows(gen, 1000, groupByEntity)
      val tempPath  = storeAsParquet(sparkSession, rows)
      val maxAge    = 86400L * 30
      val configWithMaxAge = config.copy(
        source = FileSource(tempPath, Map.empty, "eventTimestamp"),
        featureTable = config.featureTable.copy(maxAge = Some(maxAge)),
        startTime = startDate,
        endTime = endDate
      )

      val ingestionTimeUnix = System.currentTimeMillis()
      BatchPipeline.createPipeline(sparkSession, configWithMaxAge)

      val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

      rows.foreach(r => {
        val encodedEntityKey = encodeEntityKey(r, config.featureTable)
        val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
        val expectedExpiryTimestamp =
          new java.sql.Timestamp(r.eventTimestamp.getTime + 1000 * maxAge)
        storedValues should beStoredRow(
          Map(
            featureKeyEncoder("feature1") -> r.feature1,
            featureKeyEncoder("feature2") -> r.feature2,
            "_ts:test-fs"                 -> r.eventTimestamp,
            "_ex:test-fs"                 -> expectedExpiryTimestamp
          )
        )
        val keyTTL = jedis.ttl(encodedEntityKey).toLong
        keyTTL should (be <= (expectedExpiryTimestamp.getTime - ingestionTimeUnix) / 1000 and be > 0L)

      })

      val increasedMaxAge = 86400L * 60
      val configWithSecondFeatureTable = config.copy(
        source = FileSource(tempPath, Map.empty, "eventTimestamp"),
        featureTable = config.featureTable.copy(
          name = "test-fs-2",
          maxAge = Some(increasedMaxAge)
        ),
        startTime = startDate,
        endTime = endDate
      )

      val secondIngestionTimeUnix = System.currentTimeMillis()
      BatchPipeline.createPipeline(sparkSession, configWithSecondFeatureTable)

      val featureKeyEncoderSecondTable: String => String =
        encodeFeatureKey(configWithSecondFeatureTable.featureTable)

      rows.foreach(r => {
        val encodedEntityKey = encodeEntityKey(r, config.featureTable)
        val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
        val expectedExpiryTimestamp1 =
          new java.sql.Timestamp(r.eventTimestamp.getTime + 1000 * maxAge)
        val expectedExpiryTimestamp2 =
          new java.sql.Timestamp(r.eventTimestamp.getTime + 1000 * increasedMaxAge)
        storedValues should beStoredRow(
          Map(
            featureKeyEncoder("feature1")            -> r.feature1,
            featureKeyEncoder("feature2")            -> r.feature2,
            featureKeyEncoderSecondTable("feature1") -> r.feature1,
            featureKeyEncoderSecondTable("feature2") -> r.feature2,
            "_ts:test-fs"                            -> r.eventTimestamp,
            "_ts:test-fs-2"                          -> r.eventTimestamp,
            "_ex:test-fs"                            -> expectedExpiryTimestamp1,
            "_ex:test-fs-2"                          -> expectedExpiryTimestamp2
          )
        )
        val keyTTL = jedis.ttl(encodedEntityKey).toLong
        keyTTL should (be <= (expectedExpiryTimestamp2.getTime - secondIngestionTimeUnix) / 1000 and be > (expectedExpiryTimestamp1.getTime - secondIngestionTimeUnix) / 1000)

      })
    }

  "Redis key TTL" should "not be updated, when a second feature table associated with the same entity is registered and ingested, if (event_timestamp + max_age) of the second " +
    "Feature Table is not later than the expiry timestamp of the first feature table" in new Scope {
      val startDate = new DateTime().minusDays(1).withTimeAtStartOfDay()
      val endDate   = new DateTime().withTimeAtStartOfDay()
      val gen       = rowGenerator(startDate, endDate)
      val rows      = generateDistinctRows(gen, 1000, groupByEntity)
      val tempPath  = storeAsParquet(sparkSession, rows)
      val maxAge    = 86400 * 3
      val configWithMaxAge = config.copy(
        source = FileSource(tempPath, Map.empty, "eventTimestamp"),
        featureTable = config.featureTable.copy(maxAge = Some(maxAge)),
        startTime = startDate,
        endTime = endDate
      )

      val ingestionTimeUnix = System.currentTimeMillis()
      BatchPipeline.createPipeline(sparkSession, configWithMaxAge)

      val reducedMaxAge = 86400 * 2
      val configWithSecondFeatureTable = config.copy(
        source = FileSource(tempPath, Map.empty, "eventTimestamp"),
        featureTable = config.featureTable.copy(
          name = "test-fs-2",
          maxAge = Some(reducedMaxAge)
        ),
        startTime = startDate,
        endTime = endDate
      )

      BatchPipeline.createPipeline(sparkSession, configWithSecondFeatureTable)

      val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)
      val featureKeyEncoderSecondTable: String => String =
        encodeFeatureKey(configWithSecondFeatureTable.featureTable)

      rows.foreach(r => {
        val encodedEntityKey = encodeEntityKey(r, config.featureTable)
        val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
        val expectedExpiryTimestamp1 =
          new java.sql.Timestamp(r.eventTimestamp.getTime + 1000 * maxAge)
        val expectedExpiryTimestamp2 =
          new java.sql.Timestamp(r.eventTimestamp.getTime + 1000 * reducedMaxAge)
        storedValues should beStoredRow(
          Map(
            featureKeyEncoder("feature1")            -> r.feature1,
            featureKeyEncoder("feature2")            -> r.feature2,
            featureKeyEncoderSecondTable("feature1") -> r.feature1,
            featureKeyEncoderSecondTable("feature2") -> r.feature2,
            "_ts:test-fs"                            -> r.eventTimestamp,
            "_ts:test-fs-2"                          -> r.eventTimestamp,
            "_ex:test-fs"                            -> expectedExpiryTimestamp1,
            "_ex:test-fs-2"                          -> expectedExpiryTimestamp2
          )
        )
        val keyTTL = jedis.ttl(encodedEntityKey).toLong
        keyTTL should (be <= (expectedExpiryTimestamp1.getTime - ingestionTimeUnix) / 1000 and
          be > (expectedExpiryTimestamp2.getTime - ingestionTimeUnix) / 1000)

      })
    }

  "Redis key TTL" should "be updated, when the same feature table is re-ingested, with a smaller max age" in new Scope {
    val startDate = new DateTime().minusDays(1).withTimeAtStartOfDay()
    val endDate   = new DateTime().withTimeAtStartOfDay()
    val gen       = rowGenerator(startDate, endDate)
    val rows      = generateDistinctRows(gen, 1000, groupByEntity)
    val tempPath  = storeAsParquet(sparkSession, rows)
    val maxAge    = 86400 * 3
    val configWithMaxAge = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp"),
      featureTable = config.featureTable.copy(maxAge = Some(maxAge)),
      startTime = startDate,
      endTime = endDate
    )

    val ingestionTimeUnix = System.currentTimeMillis()
    BatchPipeline.createPipeline(sparkSession, configWithMaxAge)

    val reducedMaxAge = 86400 * 2
    val configWithUpdatedFeatureTable = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp"),
      featureTable = config.featureTable.copy(
        maxAge = Some(reducedMaxAge)
      ),
      startTime = startDate,
      endTime = endDate
    )

    BatchPipeline.createPipeline(sparkSession, configWithUpdatedFeatureTable)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    rows.foreach(r => {
      val encodedEntityKey = encodeEntityKey(r, config.featureTable)
      val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
      val expiryTimestampAfterUpdate =
        new java.sql.Timestamp(r.eventTimestamp.getTime + 1000 * reducedMaxAge)
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs"                 -> r.eventTimestamp,
          "_ex:test-fs"                 -> expiryTimestampAfterUpdate
        )
      )
      val keyTTL = jedis.ttl(encodedEntityKey).toLong
      keyTTL should (be <= (expiryTimestampAfterUpdate.getTime - ingestionTimeUnix) / 1000 and be > 0L)

    })
  }

  "Redis key TTL" should "be removed, when the same feature table is re-ingested without max age" in new Scope {
    val startDate = new DateTime().minusDays(1).withTimeAtStartOfDay()
    val endDate   = new DateTime().withTimeAtStartOfDay()
    val gen       = rowGenerator(startDate, endDate)
    val rows      = generateDistinctRows(gen, 1000, groupByEntity)
    val tempPath  = storeAsParquet(sparkSession, rows)
    val maxAge    = 86400 * 3
    val configWithMaxAge = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp"),
      featureTable = config.featureTable.copy(maxAge = Some(maxAge)),
      startTime = startDate,
      endTime = endDate
    )

    BatchPipeline.createPipeline(sparkSession, configWithMaxAge)

    val configWithoutMaxAge = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp"),
      startTime = startDate,
      endTime = endDate
    )

    BatchPipeline.createPipeline(sparkSession, configWithoutMaxAge)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    rows.foreach(r => {
      val encodedEntityKey = encodeEntityKey(r, config.featureTable)
      val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs"                 -> r.eventTimestamp,
          "_ex:test-fs"                 -> new Timestamp(Timestamps.MAX_VALUE.getSeconds * 1000)
        )
      )
      val keyTTL = jedis.ttl(encodedEntityKey).toInt
      keyTTL shouldEqual -1

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
      .parquet(
        Paths
          .get(deadletterConfig.deadLetterPath.get, sparkSession.conf.get("spark.app.id"))
          .toString
      )
      .count() should be(rows.length)

    SparkEnv.get.metricsSystem.report()
    statsDStub.receivedMetrics should contain.allElementsOf(
      Map(
        "driver.ingestion_pipeline.deadletter_count" -> rows.length
      )
    )
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

  "Spark expressions" should "be allowed in mapping" in new Scope {
    val gen  = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 100, groupByEntity)

    val tempPath = storeAsParquet(sparkSession, rows)

    val configWithMapping = config.copy(
      source = FileSource(
        tempPath,
        Map(
          "feature1" -> "feature1 + 1",
          "feature2" -> "feature1 + feature2 * 2"
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
          featureKeyEncoder("feature1") -> (r.feature1 + 1),
          featureKeyEncoder("feature2") -> (r.feature1 + r.feature2 * 2),
          "_ts:test-fs"                 -> r.eventTimestamp
        )
      )
    })
  }
}
