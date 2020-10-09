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

import feast.ingestion.registry.proto.ProtoRegistryFactory
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import feast.ingestion.utils.ProtoReflection
import feast.ingestion.validation.RowValidator
import org.apache.spark.sql.streaming.StreamingQuery

object StreamingPipeline extends BasePipeline with Serializable {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    import sparkSession.implicits._

    val featureTable = config.featureTable
    val projection =
      inputProjection(config.source, featureTable.features, featureTable.entities)
    val validator = new RowValidator(featureTable, config.source.timestampColumn)

    val messageParser =
      protoParser(sparkSession, config.source.asInstanceOf[StreamingSource].classpath)

    val input = config.source match {
      case source: KafkaSource =>
        sparkSession.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", source.bootstrapServers)
          .option("subscribe", source.topic)
          .load()
    }

    val projected = input
      .withColumn("features", messageParser($"value"))
      .select("features.*")
      .select(projection: _*)

    val query = projected.writeStream
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        batchDF.persist()

        val validRows = batchDF
          .filter(validator.checkAll)

        validRows.write
          .format("feast.ingestion.stores.redis")
          .option("entity_columns", featureTable.entities.map(_.name).mkString(","))
          .option("namespace", featureTable.name)
          .option("project_name", featureTable.project)
          .option("timestamp_column", config.source.timestampColumn)
          .save()

        config.deadLetterPath match {
          case Some(path) =>
            batchDF
              .filter(!validator.checkAll)
              .write
              .format("parquet")
              .mode(SaveMode.Append)
              .save(path)
          case _ =>
            batchDF
              .filter(!validator.checkAll)
              .foreach(r => {
                println(s"Row failed validation $r")
              })
        }

        batchDF.unpersist()
        () // return Unit to avoid compile error with overloaded foreachBatch
      }
      .start()

    Some(query)
  }

  private def protoParser(sparkSession: SparkSession, className: String) = {
    val protoRegistry = ProtoRegistryFactory.resolveProtoRegistry(sparkSession)

    val parser: Array[Byte] => Row = ProtoReflection.createMessageParser(protoRegistry, className)

    udf(parser, ProtoReflection.inferSchema(protoRegistry.getProtoDescriptor(className)))
  }
}
