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
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.apache.kafka.clients.producer._
import com.example.protos.{TestMessage, VehicleType}
import com.google.protobuf.Timestamp

class StreamingPipelineIT extends UnitSpec with ForAllTestContainer {
  val redisContainer = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))
  val kafkaContainer = KafkaContainer()

  override val container = MultipleContainers(redisContainer, kafkaContainer)

  trait SparkContext {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Testing")
      .set("spark.redis.host", redisContainer.host)
      .set("spark.default.parallelism", "8")
      .set("spark.redis.port", redisContainer.mappedPort(6379).toString)

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

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

  trait Scope extends SparkContext with KafkaPublisher {
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

  "Streaming pipeline" should "stream" in new Scope {
    val kafkaSource = KafkaSource(
      bootstrapServers = kafkaContainer.bootstrapServers,
      topic = "topic",
      classpath = "com.example.protos.TestMessage",
      mapping = Map.empty,
      timestampColumn = "event_timestamp"
    )

    val configWithKafka = config.copy(source = kafkaSource)

    val m = TestMessage
      .newBuilder()
      .setS2Id(1)
      .setUniqueDrivers(100)
      .setVehicleType(VehicleType.Enum.BIKE)
      .setEventTimestamp(Timestamp.newBuilder().setSeconds(DateTime.now().getMillis / 1000).build())
      .build

    val query = StreamingPipeline.createPipeline(sparkSession, configWithKafka).get

    sendToKafka(kafkaSource.topic, m)

    query.processAllAvailable()
  }
}
