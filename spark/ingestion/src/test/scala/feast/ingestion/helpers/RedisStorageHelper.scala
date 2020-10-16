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
package feast.ingestion.helpers

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

import com.google.protobuf.Timestamp
import feast.ingestion.FeatureTable
import feast.proto.types.ValueProto
import feast.ingestion.utils.TypeConversion._
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.must.Matchers.contain
import com.google.common.hash.Hashing

object RedisStorageHelper {
  def encodeFeatureKey(featureTable: FeatureTable)(feature: String): String = {
    val fullReference = s"${featureTable.name}:$feature"
    Hashing.murmur3_32.hashString(fullReference, StandardCharsets.UTF_8).asInt.toHexString
  }

  def beStoredRow(mappedRow: Map[String, Any]): Matcher[Map[Array[Byte], Array[Byte]]] = {
    val m: Matcher[Map[String, Any]] = contain.allElementsOf(mappedRow).matcher

    m compose {
      (_: Map[Array[Byte], Array[Byte]])
        .map { case (k, v) =>
          if (k.length == 4)
            (
              ByteBuffer.wrap(k).order(ByteOrder.LITTLE_ENDIAN).getInt.toHexString,
              ValueProto.Value.parseFrom(v).asScala
            )
          else
            (new String(k), Timestamp.parseFrom(v).asScala)
        }
    }
  }
}
