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

import feast.ingestion.Modes.Modes
import org.joda.time.DateTime

object Modes extends Enumeration {
  type Modes = Value
  val Offline, Online = Value
}

abstract class StoreConfig

case class RedisConfig(host: String, port: Int) extends StoreConfig

abstract class MetricConfig

case class StatsDConfig(host: String, port: Int) extends MetricConfig

abstract class Source {
  def mapping: Map[String, String]

  def timestampColumn: String
}

abstract class BatchSource extends Source

abstract class StreamingSource extends Source {
  def classpath: String
}

case class FileSource(
    path: String,
    override val mapping: Map[String, String],
    override val timestampColumn: String
) extends BatchSource

case class BQSource(
    project: String,
    dataset: String,
    table: String,
    override val mapping: Map[String, String],
    override val timestampColumn: String
) extends BatchSource

case class KafkaSource(
    bootstrapServers: String,
    topic: String,
    override val classpath: String,
    override val mapping: Map[String, String],
    override val timestampColumn: String
) extends StreamingSource

case class Sources(
    file: Option[FileSource] = None,
    bq: Option[BQSource] = None,
    kafka: Option[KafkaSource] = None
)

case class Field(name: String, `type`: feast.proto.types.ValueProto.ValueType.Enum)

case class FeatureTable(
    name: String,
    project: String,
    entities: Seq[Field],
    features: Seq[Field]
)

case class IngestionJobConfig(
    mode: Modes = Modes.Offline,
    featureTable: FeatureTable = null,
    source: Source = null,
    startTime: DateTime = DateTime.now(),
    endTime: DateTime = DateTime.now(),
    store: StoreConfig = RedisConfig("localhost", 6379),
    metrics: Option[MetricConfig] = Some(StatsDConfig("localhost", 9125)),
    deadLetterPath: Option[String] = None
)
