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
import java.io.{IOException, ObjectInputStream}

import com.google.protobuf.Descriptors.Descriptor

import collection.mutable
import scala.util.control.NonFatal

class LocalProtoRegistry extends ProtoRegistry {
  @transient
  private var cache: mutable.Map[String, Descriptor] = mutable.Map.empty

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = {
    try {
      ois.defaultReadObject()
      cache = mutable.Map.empty
    } catch {
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }

  override def getProtoDescriptor(className: String): Descriptor = {
    if (!cache.contains(className)) {
      cache(className) = Class
        .forName(className, true, getClass.getClassLoader)
        .getMethod("getDescriptor")
        .invoke(null)
        .asInstanceOf[Descriptor]
    }

    cache(className)
  }
}
