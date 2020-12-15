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
import java.util.Date

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import feast.ingestion.helpers.DataHelper.generateTempPath
import feast.ingestion.utils.testing.MemoryStreamingSource
import feast.proto.storage.RedisProto.RedisKeyV2
import feast.proto.types.ValueProto
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.apache.spark.api.python.DynamicPythonFunction
import org.apache.spark.sql.{Encoder, Row, SQLContext}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.functions.struct
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters._
import scala.util.Random

class PandasUDF extends SparkSpec with ForAllTestContainer {
  case class TestRow(key: String, num: Int, num2: Int, timestamp: java.sql.Timestamp)

  override val container = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.redis.host", container.host)
    .set("spark.redis.port", container.mappedPort(6379).toString)

  trait Scope {
    implicit def testRowEncoder: Encoder[TestRow] = ExpressionEncoder()
    implicit def sqlContext: SQLContext           = sparkSession.sqlContext

    val jedis = new Jedis("localhost", container.mappedPort(6379))
    jedis.flushAll()

    val SQL_SCALAR_PANDAS_UDF = 200
    val rand                  = new Random()

    def encodeEntityKey(key: String): Array[Byte] =
      RedisKeyV2
        .newBuilder()
        .setProject("default")
        .addAllEntityNames(Seq("key").asJava)
        .addEntityValues(ValueProto.Value.newBuilder().setStringVal(key))
        .build
        .toByteArray

    // Function checks that num between 0 and 10 and num2 between 0 and 20
    // See the code test/resources/python/udf.py
    val pickledCode = getClass.getResourceAsStream("/python/udf.pickle").readAllBytes()
    val pythonFun   = DynamicPythonFunction.create(pickledCode)

    val udf: UserDefinedPythonFunction = UserDefinedPythonFunction(
      "validate",
      pythonFun,
      BooleanType,
      SQL_SCALAR_PANDAS_UDF,
      udfDeterministic = true
    )

    val inputData = MemoryStream[TestRow]
    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        project = "default",
        entities = Seq(Field("key", ValueType.Enum.STRING)),
        features = Seq(
          Field("num", ValueType.Enum.INT32),
          Field("num2", ValueType.Enum.INT32)
        )
      ),
      source = MemoryStreamingSource(inputData),
      validationConfig = Some(
        ValidationConfig(
          name = "testFun",
          pickledCodePath = getClass.getResource("/python/udf.pickle").getPath,
          includeArchivePath = getClass.getResource("/python/libs.tar.gz").getPath
        )
      ),
      doNotIngestInvalidRows = true,
      deadLetterPath = Some(generateTempPath("deadletters"))
    )
  }

  "Custom Python code" should "be applied in streaming pipeline" in new Scope {
    sparkSession.sparkContext.addFile(getClass.getResource("/python/libs.tar.gz").getPath)

    val df = inputData.toDF()

    val cols = df.columns.map(df(_))
    val streamingQuery = df
      .withColumn("valid", udf(struct(cols: _*)))
      .writeStream
      .format("memory")
      .queryName("sink")
      .start()

    val data =
      (1 to 100).map(_ => TestRow(rand.nextString(5), rand.nextInt(100), rand.nextInt(100), null))
    inputData.addData(data)
    streamingQuery.processAllAvailable()

    val expected = data
      .map(testRow => Row(testRow.num, testRow.num2, testRow.num <= 10 && testRow.num2 <= 20))
      .toArray

    val output = sparkSession.sql("select num, num2, valid from sink").collect()
    output should be(expected)
  }

  "Custom Python code" should "be used for data validation in StreamingPipeline" in new Scope {
    val query = StreamingPipeline.createPipeline(sparkSession, config).get

    val ts = new Timestamp(new Date().getTime)
    val data =
      (1 to 1000).map(_ => TestRow(rand.nextString(5), rand.nextInt(100), rand.nextInt(100), ts))
    inputData.addData(data)
    query.processAllAvailable()

    // Map (key -> isValid)
    val expected = data
      .map(testRow => testRow.key -> (testRow.num <= 10 && testRow.num2 <= 20))
      .toMap

    // Invalid Rows stored to DeadLetters
    sparkSession.read
      .parquet(
        Paths
          .get(config.deadLetterPath.get, sparkSession.conf.get("spark.app.id"))
          .toString
      )
      .count() should be(expected.count(!_._2))

    // Valid rows saved to Storage
    expected.filter(_._2).keys.foreach { key =>
      {
        jedis.hgetAll(encodeEntityKey(key)).asScala.toMap should not be (Map.empty)
      }
    }
  }
}
