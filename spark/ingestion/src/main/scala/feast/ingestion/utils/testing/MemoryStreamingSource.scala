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
package feast.ingestion.utils.testing

import feast.ingestion.{DataFormat, StreamingSource}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream

// For test purposes
case class MemoryStreamingSource(
    stream: MemoryStream[_],
    override val fieldMapping: Map[String, String] = Map.empty,
    override val eventTimestampColumn: String = "timestamp",
    override val createdTimestampColumn: Option[String] = None,
    override val datePartitionColumn: Option[String] = None
) extends StreamingSource {
  def read: DataFrame = stream.toDF()

  override def format: DataFormat = null
}
