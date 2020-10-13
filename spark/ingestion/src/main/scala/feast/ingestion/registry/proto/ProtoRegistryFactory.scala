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
package feast.ingestion.registry.proto

import org.apache.spark.sql.SparkSession

object ProtoRegistryFactory {
  val CONFIG_PREFIX       = "feast.ingestion.registry.proto."
  val PROTO_REGISTRY_KIND = s"${CONFIG_PREFIX}kind"
  val DEFAULT_KIND        = "local"

  def resolveProtoRegistry(sparkSession: SparkSession): ProtoRegistry = {
    val config     = sparkSession.sparkContext.getConf
    val kind       = config.get(PROTO_REGISTRY_KIND, DEFAULT_KIND)
    val properties = config.getAllWithPrefix(CONFIG_PREFIX).toMap
    protoRegistry(kind, properties)
  }

  private def protoRegistry(name: String, properties: Map[String, String]): ProtoRegistry =
    name match {
      case "local"   => new LocalProtoRegistry
      case "stencil" => new StencilProtoRegistry(properties("url"))
    }
}
