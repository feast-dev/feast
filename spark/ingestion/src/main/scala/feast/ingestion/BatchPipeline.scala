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

import feast.ingestion.sources.bq.BigQueryReader
import feast.ingestion.sources.file.FileReader
import feast.ingestion.validation.{RowValidator, TypeCheck}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Batch Ingestion Flow:
  * 1. Read from source (BQ | File)
  * 2. Map source columns to FeatureTable's schema
  * 3. Validate
  * 4. Store valid rows in redis
  * 5. Store invalid rows in parquet format at `deadletter` destination
  */
object BatchPipeline extends BasePipeline {
  override def createPipeline(sparkSession: SparkSession, config: IngestionJobConfig) = {
    val featureTable = config.featureTable
    val projection =
      inputProjection(config.source, featureTable.features, featureTable.entities)
    val validator = new RowValidator(featureTable, config.source.eventTimestampColumn)

    val input = config.source match {
      case source: BQSource =>
        BigQueryReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
      case source: FileSource =>
        FileReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
    }

    val projected = input.select(projection: _*).cache()

    TypeCheck.allTypesMatch(projected.schema, featureTable) match {
      case Some(error) =>
        throw new RuntimeException(s"Dataframe columns don't match expected feature types: $error")
      case _ => ()
    }

    val validRows = projected
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
        projected
          .filter(!validator.checkAll)
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .save(Paths.get(path, SparkEnv.get.conf.getAppId).toString)
      case _ => None
    }

    None
  }
}
