/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

public class JsonUtil {

  private static Gson gson = new Gson();

  /**
   * Unmarshals a given json string to map
   *
   * @param jsonString valid json formatted string
   * @return map of keys to values in json
   */
  public static Map<String, String> convertJsonStringToMap(String jsonString) {
    if (jsonString == null || jsonString.equals("") || jsonString.equals("{}")) {
      return Collections.emptyMap();
    }
    Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
    return gson.fromJson(jsonString, stringMapType);
  }
}
