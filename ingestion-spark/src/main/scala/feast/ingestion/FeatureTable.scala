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

abstract class Source {
  def mapping: Map[String, String]

  def timestampColumn: String
}

abstract class OfflineSource extends Source

abstract class OnlineSource extends Source

case class GSSource(
    path: String,
    override val mapping: Map[String, String],
    override val timestampColumn: String
) extends OfflineSource

case class BQSource(
    project: String,
    dataset: String,
    table: String,
    override val mapping: Map[String, String],
    override val timestampColumn: String
) extends OfflineSource

case class KafkaSource(
    bootstrapServers: String,
    topic: String,
    override val mapping: Map[String, String],
    override val timestampColumn: String
) extends OnlineSource

case class Field(name: String, `type`: feast.proto.types.ValueProto.ValueType.Enum)

case class FeatureTable(
    name: String,
    project: String,
    entities: Seq[Field],
    features: Seq[Field],
    offline_source: Option[OfflineSource] = None,
    online_source: Option[OnlineSource] = None
)
