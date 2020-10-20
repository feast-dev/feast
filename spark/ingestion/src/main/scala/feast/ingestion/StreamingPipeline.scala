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
import feast.ingestion.validation.{RowValidator, TypeCheck}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.avro._

/**
  * Streaming pipeline (currently in micro-batches mode only, since we need to have multiple sinks: redis & deadletters).
  * Flow:
  * 1. Read from streaming source (currently only Kafka)
  * 2. Parse bytes from streaming source into Row with schema inferenced from provided class (Protobuf)
  * 3. Map columns according to provided mapping rules
  * 4. Validate
  * 5. (In batches) store to redis valid rows / write to deadletter (parquet) invalid
  */
object StreamingPipeline extends BasePipeline with Serializable {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    import sparkSession.implicits._

    val featureTable = config.featureTable
    val projection =
      inputProjection(config.source, featureTable.features, featureTable.entities)
    val validator = new RowValidator(featureTable, config.source.eventTimestampColumn)

    val input = config.source match {
      case source: KafkaSource =>
        sparkSession.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", source.bootstrapServers)
          .option("subscribe", source.topic)
          .load()
    }

    val parsed = config.source.asInstanceOf[StreamingSource].format match {
      case ProtoFormat(classPath) =>
        val parser = protoParser(sparkSession, classPath)
        input.withColumn("features", parser($"value"))
      case AvroFormat(schemaJson) =>
        input.select(from_avro($"value", schemaJson).alias("features"))
    }

    val projected = parsed
      .select("features.*")
      .select(projection: _*)

    TypeCheck.allTypesMatch(projected.schema, featureTable) match {
      case Some(error) =>
        throw new RuntimeException(s"Dataframe columns don't match expected feature types: $error")
      case _ => ()
    }

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
          .option("timestamp_column", config.source.eventTimestampColumn)
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
