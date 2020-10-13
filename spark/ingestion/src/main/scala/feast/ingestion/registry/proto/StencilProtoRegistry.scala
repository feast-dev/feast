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
import java.util.Collections

import com.google.protobuf.Descriptors
import com.gojek.de.stencil.StencilClientFactory
import com.gojek.de.stencil.client.StencilClient

class StencilProtoRegistry(val url: String) extends ProtoRegistry {

  val stencilClient: StencilClient =
    StencilClientFactory.getClient(url, Collections.emptyMap[String, String])

  override def getProtoDescriptor(className: String): Descriptors.Descriptor = {
    stencilClient.get(className)
  }
}
