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
package feast.ingestion.registry

import com.example.protos.TestMessage
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import feast.ingestion.helpers.DataHelper.generateTempPath
import feast.ingestion.registry.proto.ProtoRegistryFactory
import feast.ingestion.utils.ProtoReflection
import feast.ingestion.SparkSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.BeforeAndAfterAll

case class BinaryRecord(bytes: Array[Byte])

class StencilSpec extends SparkSpec with BeforeAndAfterAll {
  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.sql.streaming.checkpointLocation", generateTempPath("checkpoint"))
    .set("feast.ingestion.registry.proto.kind", "stencil")
    .set(
      "feast.ingestion.registry.proto.url",
      s"http://localhost:${wireMockConfig.portNumber()}/source.desc"
    )

  val wireMockConfig = (new WireMockConfiguration)
    .withRootDirectory(getClass.getResource("/stencil").getPath)
    .port(8082)
  val wireMockServer = new WireMockServer(wireMockConfig)

  override def beforeAll(): Unit =
    wireMockServer.start()

  override def afterAll(): Unit =
    wireMockServer.stop()

  trait Scope {
    implicit val encoder: ExpressionEncoder[BinaryRecord] = ExpressionEncoder[BinaryRecord]
  }

  "Proto parser" should "be able to retrieve proto descriptors from external repos" in new Scope {
    val testMessage = TestMessage
      .newBuilder()
      .setS2Id(1)
      .setUniqueDrivers(100)
      .build()

    val protoRegistry = ProtoRegistryFactory.resolveProtoRegistry(sparkSession)
    val className     = "com.example.protos.TestMessage"

    val parser: Array[Byte] => Row = ProtoReflection.createMessageParser(protoRegistry, className)
    val parseUDF =
      udf(parser, ProtoReflection.inferSchema(protoRegistry.getProtoDescriptor(className)))

    val parsed = sparkSession
      .createDataset(Seq(BinaryRecord(testMessage.toByteArray)))
      .withColumn("parsed", parseUDF(col("bytes")))
      .select("parsed.*")

    parsed.schema should be(
      StructType(
        StructField("s2_id", LongType, true) ::
          StructField("vehicle_type", StringType, true) ::
          StructField("unique_drivers", LongType, true) ::
          StructField("event_timestamp", TimestampType, true) :: Nil
      )
    )

    parsed.collect() should contain(
      Row(
        1L,
        null,
        100,
        null
      )
    )
  }
}
