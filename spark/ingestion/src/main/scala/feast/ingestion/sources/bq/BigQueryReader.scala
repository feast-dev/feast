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
package feast.ingestion.sources.bq

import java.sql.Timestamp

import feast.ingestion.BQSource
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col

object BigQueryReader {
  def createBatchSource(
      sqlContext: SQLContext,
      source: BQSource,
      start: DateTime,
      end: DateTime
  ): DataFrame = {
    val reader = sqlContext.read
      .format("bigquery")
      .option("viewsEnabled", "true")

    source.materialization foreach { materializationConfig =>
      reader
        .option("materializationProject", materializationConfig.project)
        .option("materializationDataset", materializationConfig.dataset)
    }

    reader
      .load(s"${source.project}.${source.dataset}.${source.table}")
      .filter(col(source.eventTimestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.eventTimestampColumn) < new Timestamp(end.getMillis))
  }
}
