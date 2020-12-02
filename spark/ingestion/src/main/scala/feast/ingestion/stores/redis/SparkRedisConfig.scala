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
package feast.ingestion.stores.redis

case class SparkRedisConfig(
    namespace: String,
    projectName: String,
    entityColumns: Array[String],
    timestampColumn: String,
    iteratorGroupingSize: Int = 1000,
    timestampPrefix: String = "_ts",
    repartitionByEntity: Boolean = true,
    maxAge: Long = 0,
    expiryPrefix: String = "_ex"
)

object SparkRedisConfig {
  val NAMESPACE          = "namespace"
  val ENTITY_COLUMNS     = "entity_columns"
  val TS_COLUMN          = "timestamp_column"
  val ENTITY_REPARTITION = "entity_repartition"
  val PROJECT_NAME       = "project_name"
  val MAX_AGE            = "max_age"

  def parse(parameters: Map[String, String]): SparkRedisConfig =
    SparkRedisConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      projectName = parameters.getOrElse(PROJECT_NAME, "default"),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp"),
      repartitionByEntity = parameters.getOrElse(ENTITY_REPARTITION, "true") == "true",
      maxAge = parameters.get(MAX_AGE).map(_.toLong).getOrElse(0)
    )
}
