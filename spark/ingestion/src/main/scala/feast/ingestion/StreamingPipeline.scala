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

import java.io.File
import java.util.concurrent.TimeUnit

import feast.ingestion.metrics.IngestionPipelineMetrics
import feast.ingestion.registry.proto.ProtoRegistryFactory
import org.apache.spark.sql.{DataFrame, Encoder, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{expr, struct, udf}
import feast.ingestion.utils.ProtoReflection
import feast.ingestion.utils.testing.MemoryStreamingSource
import feast.ingestion.validation.{RowValidator, TypeCheck}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkEnv, SparkFiles}
import org.apache.spark.api.python.DynamicPythonFunction
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.avro._
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.encoders.RowEncoder

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
    val rowValidator  = new RowValidator(featureTable, config.source.eventTimestampColumn)
    val metrics       = new IngestionPipelineMetrics
    val validationUDF = createValidationUDF(sparkSession, config)

    val input = config.source match {
      case source: KafkaSource =>
        sparkSession.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", source.bootstrapServers)
          .option("subscribe", source.topic)
          .load()
      case source: MemoryStreamingSource =>
        source.read
    }

    val parsed = config.source.asInstanceOf[StreamingSource].format match {
      case ProtoFormat(classPath) =>
        val parser = protoParser(sparkSession, classPath)
        input.withColumn("features", parser($"value"))
      case AvroFormat(schemaJson) =>
        input.select(from_avro($"value", schemaJson).alias("features"))
      case _ =>
        val columns = input.columns.map(input(_))
        input.select(struct(columns: _*).alias("features"))
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
        val rowsAfterValidation = if (validationUDF.nonEmpty) {
          val columns = batchDF.columns.map(batchDF(_))
          batchDF.withColumn(
            "_isValid",
            rowValidator.allChecks && validationUDF.get(struct(columns: _*))
          )
        } else {
          batchDF.withColumn("_isValid", rowValidator.allChecks)
        }
        rowsAfterValidation.persist()
        implicit def rowEncoder: Encoder[Row] = RowEncoder(rowsAfterValidation.schema)

        rowsAfterValidation
          .mapPartitions(metrics.incrementRead)
          .filter(if (config.doNotIngestInvalidRows) expr("_isValid") else rowValidator.allChecks)
          .write
          .format("feast.ingestion.stores.redis")
          .option("entity_columns", featureTable.entities.map(_.name).mkString(","))
          .option("namespace", featureTable.name)
          .option("project_name", featureTable.project)
          .option("timestamp_column", config.source.eventTimestampColumn)
          .option("max_age", config.featureTable.maxAge.getOrElse(0L))
          .save()

        config.deadLetterPath match {
          case Some(path) =>
            rowsAfterValidation
              .filter("!_isValid")
              .mapPartitions(metrics.incrementDeadLetters)
              .write
              .format("parquet")
              .mode(SaveMode.Append)
              .save(StringUtils.stripEnd(path, "/") + "/" + SparkEnv.get.conf.getAppId)
          case _ =>
            rowsAfterValidation
              .filter("!_isValid")
              .foreach(r => {
                println(s"Row failed validation $r")
              })
        }

        rowsAfterValidation.unpersist()
        () // return Unit to avoid compile error with overloaded foreachBatch
      }
      .trigger(ProcessingTimeTrigger.create(config.streamingTriggeringSecs, TimeUnit.SECONDS))
      .start()

    Some(query)
  }

  private def protoParser(sparkSession: SparkSession, className: String) = {
    val protoRegistry = ProtoRegistryFactory.resolveProtoRegistry(sparkSession)

    val parser: Array[Byte] => Row = ProtoReflection.createMessageParser(protoRegistry, className)

    // ToDo: create correctly typed parser
    // spark deprecated returnType argument, instead it will infer it from udf function signature
    udf(parser, ProtoReflection.inferSchema(protoRegistry.getProtoDescriptor(className)))
  }

  private def createValidationUDF(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[UserDefinedPythonFunction] =
    config.validationConfig.map { validationConfig =>
      if (validationConfig.includeArchivePath.nonEmpty) {
        val archivePath =
          DynamicPythonFunction.libsPathWithPlatform(validationConfig.includeArchivePath)
        sparkSession.sparkContext.addFile(archivePath)
      }

      // this is the trick to download remote file on the driver
      // after file added to sparkContext it will be immediately fetched to local dir (accessible via SparkFiles)
      sparkSession.sparkContext.addFile(validationConfig.pickledCodePath)
      val fileName    = validationConfig.pickledCodePath.split("/").last
      val pickledCode = FileUtils.readFileToByteArray(new File(SparkFiles.get(fileName)))

      UserDefinedPythonFunction(
        validationConfig.name,
        DynamicPythonFunction.create(pickledCode),
        BooleanType,
        pythonEvalType = 200, // SQL_SCALAR_PANDAS_UDF (original constant is in private object)
        udfDeterministic = true
      )
    }
}
