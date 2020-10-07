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

import java.sql

import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.{AbstractMessage, ByteString, GeneratedMessageV3, Parser, Timestamp}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import feast.ingestion.BatchPipeline.inputProjection
import feast.ingestion.utils.ProtoReflection
import feast.ingestion.validation.RowValidator
import org.apache.spark.sql.streaming.StreamingQuery

import scala.collection.convert.ImplicitConversions._
import collection.JavaConverters._

object StreamingPipeline extends BasePipeline with Serializable {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    import sparkSession.implicits._

    val featureTable = config.featureTable
    val projection =
      inputProjection(config.source, featureTable.features, featureTable.entities)
    val validator = new RowValidator(featureTable)

    val defaultInstance = defaultInstanceFromProtoClass(
      config.source.asInstanceOf[StreamingSource].classpath
    )
    val protoParser = udf(
      ProtoReflection.createMessageParser(defaultInstance),
      inferSchemaFromProto(defaultInstance)
    )

    val input = config.source match {
      case source: KafkaSource =>
        sparkSession.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", source.bootstrapServers)
          .option("subscribe", source.topic)
          .load()
    }

    val projected = input
      .withColumn("features", protoParser($"value"))
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

  private def defaultInstanceFromProtoClass(className: String): GeneratedMessageV3 =
    Class
      .forName(className, true, getClass.getClassLoader)
      .asInstanceOf[Class[GeneratedMessageV3]]
      .getMethod("getDefaultInstance")
      .invoke(null)
      .asInstanceOf[GeneratedMessageV3]

  private def inferSchemaFromProto(defaultInstance: GeneratedMessageV3) =
    StructType(
      defaultInstance.getDescriptorForType.getFields.flatMap(ProtoReflection.structFieldFor)
    )

}
