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
package feast.ingestion.sources.file

import java.sql.{Timestamp, Date}

import feast.ingestion.FileSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime

object FileReader {
  def createBatchSource(
      sqlContext: SQLContext,
      source: FileSource,
      start: DateTime,
      end: DateTime
  ): DataFrame = {
    val reader = sqlContext.read
      .parquet(source.path)
      .filter(col(source.eventTimestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.eventTimestampColumn) < new Timestamp(end.getMillis))

    source.datePartitionColumn match {
      case Some(partitionColumn) if partitionColumn.nonEmpty =>
        reader
          .filter(col(partitionColumn) >= new Date(start.getMillis))
          .filter(col(partitionColumn) <= new Date(end.getMillis))
      case _ => reader
    }
  }
}
