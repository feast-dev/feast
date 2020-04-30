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
package feast.core.util;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;

public class TypeConversion {
  private static Gson gson = new Gson();

  /**
   * Convert a java data object to protobuf Timestamp object
   *
   * @param ts timestamp
   * @return protobuf.Timestamp object of the given timestamp
   */
  public static com.google.protobuf.Timestamp convertTimestamp(Date ts) {
    return com.google.protobuf.Timestamp.newBuilder().setSeconds(ts.getTime() / 1000).build();
  }

  /**
   * Convert a string of comma-separated strings to list of strings
   *
   * @param tags comma separated tags
   * @return list of tags
   */
  public static List<String> convertTagStringToList(String tags) {
    if (tags == null || tags.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(tags.split(","));
  }

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

  /**
   * Marshals a given map into its corresponding json string
   *
   * @param map
   * @return json string corresponding to given map
   */
  public static String convertMapToJsonString(Map<String, String> map) {
    return gson.toJson(map);
  }

  /**
   * Convert a map of key value pairs to a array of java arguments in format --key=value
   *
   * @param map
   * @return array of string arguments
   */
  public static String[] convertMapToArgs(Map<String, String> map) {
    List<String> args = new ArrayList<>();
    for (Entry<String, String> arg : map.entrySet()) {
      args.add(Strings.lenientFormat("--%s=%s", arg.getKey(), arg.getValue()));
    }
    return args.toArray(new String[] {});
  }
}
