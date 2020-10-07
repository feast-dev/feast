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

import java.util.Properties

import com.dimafeng.testcontainers.{
  ForAllTestContainer,
  GenericContainer,
  KafkaContainer,
  MultipleContainers
}
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.joda.time.DateTime
import org.apache.kafka.clients.producer._
import com.example.protos.{TestMessage, VehicleType}
import com.google.protobuf.Timestamp
import org.scalacheck.Gen
import redis.clients.jedis.Jedis

import collection.JavaConverters._

import feast.ingestion.helpers.RedisStorageHelper._
import feast.ingestion.helpers.DataHelper._

class StreamingPipelineIT extends SparkSpec with ForAllTestContainer {
  val redisContainer = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))
  val kafkaContainer = KafkaContainer()

  override val container = MultipleContainers(redisContainer, kafkaContainer)
  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.redis.host", redisContainer.host)
    .set("spark.redis.port", redisContainer.mappedPort(6379).toString)
    .set("spark.sql.streaming.checkpointLocation", generateTempPath("checkpoint"))

  trait KafkaPublisher {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaContainer.bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.VoidSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    val producer = new KafkaProducer[Void, Array[Byte]](props)

    def sendToKafka(topic: String, m: TestMessage): Unit = {
      producer.send(new ProducerRecord[Void, Array[Byte]](topic, null, m.toByteArray))
    }
  }

  trait Scope extends KafkaPublisher {
    val jedis = new Jedis("localhost", redisContainer.mappedPort(6379))
    jedis.flushAll()

    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "driver-fs",
        project = "default",
        entities = Seq(
          Field("s2_id", ValueType.Enum.INT32),
          Field("vehicle_type", ValueType.Enum.STRING)
        ),
        features = Seq(
          Field("unique_drivers", ValueType.Enum.INT64)
        )
      )
    )

    def encodeEntityKey(row: TestMessage, featureTable: FeatureTable): Array[Byte] = {
      val entityPrefix = featureTable.entities.map(_.name).sorted.mkString("_")
      s"${featureTable.project}_${entityPrefix}:${row.getS2Id}:${row.getVehicleType}".getBytes
    }

    def groupByEntity(row: TestMessage) =
      new String(encodeEntityKey(row, config.featureTable))

    def rowGenerator =
      for {
        s2_id        <- Gen.chooseNum(0, 1000000)
        vehicle_type <- Gen.oneOf(VehicleType.Enum.BIKE, VehicleType.Enum.CAR)
        drivers      <- Gen.chooseNum(0, 1000)
        eventTimestamp <- Gen
          .choose(0, 300)
          .map(DateTime.now.withMillisOfSecond(0).minusSeconds)
      } yield TestMessage.newBuilder
        .setS2Id(s2_id)
        .setVehicleType(vehicle_type)
        .setUniqueDrivers(drivers)
        .setEventTimestamp(Timestamp.newBuilder.setSeconds(eventTimestamp.getMillis / 1000))
        .build

    val kafkaSource = KafkaSource(
      bootstrapServers = kafkaContainer.bootstrapServers,
      topic = "topic",
      classpath = "com.example.protos.TestMessage",
      mapping = Map.empty,
      timestampColumn = "event_timestamp"
    )
    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)
  }

  "Streaming pipeline" should "store valid proto messages from kafka to redis" in new Scope {
    val configWithKafka = config.copy(source = kafkaSource)
    val query           = StreamingPipeline.createPipeline(sparkSession, configWithKafka).get
    query.processAllAvailable() // to init kafka consumer

    val rows = generateDistinctRows(rowGenerator, 1000, groupByEntity)
    rows.foreach(sendToKafka(kafkaSource.topic, _))

    query.processAllAvailable()

    rows.foreach { r =>
      val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("unique_drivers") -> r.getUniqueDrivers,
          "_ts:driver-fs"                     -> new java.sql.Timestamp(r.getEventTimestamp.getSeconds * 1000)
        )
      )
    }
  }

  "Streaming pipeline" should "store invalid proto messages to deadletter path" in new Scope {
    val configWithDeadletter = config.copy(
      source = kafkaSource,
      deadLetterPath = Some(generateTempPath("deadletters"))
    )
    val query = StreamingPipeline.createPipeline(sparkSession, configWithDeadletter).get
    query.processAllAvailable() // to init kafka consumer

    val rows = generateDistinctRows(rowGenerator, 1000, groupByEntity).map(
      _.toBuilder.clearVehicleType().build()
    )

    rows.foreach(sendToKafka(kafkaSource.topic, _))
    query.processAllAvailable()

    // ingest twice to check that rows are appended to deadletter path
    rows.foreach(sendToKafka(kafkaSource.topic, _))
    query.processAllAvailable()

    sparkSession.read
      .parquet(configWithDeadletter.deadLetterPath.get)
      .count() should be(2 * rows.length)
  }
}
