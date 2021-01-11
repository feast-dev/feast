/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.ingestion.utils

import java.util.Locale.ENGLISH

import org.json4s.{JArray, JField, JObject, JValue}

object JsonUtils {
  def mapFieldWithParent(jv: JValue)(f: (String, JField) => JField): JValue = {
    def rec(v: JValue, parent: String = ""): JValue = v match {
      case JObject(l) => JObject(l.map { case (key, va) => f(parent, key -> rec(va, key)) })
      case JArray(l)  => JArray(l.map(rec(_, parent)))
      case x          => x
    }
    rec(jv)
  }

  def camelize(word: String): String = {
    if (word.nonEmpty) {
      val w = pascalize(word)
      w.substring(0, 1).toLowerCase(ENGLISH) + w.substring(1)
    } else {
      word
    }
  }

  def pascalize(word: String): String = {
    val lst = word.split("_").toList
    (lst.headOption.map(s => s.substring(0, 1).toUpperCase(ENGLISH) + s.substring(1)).get ::
      lst.tail.map(s => s.substring(0, 1).toUpperCase + s.substring(1))).mkString("")
  }
}
