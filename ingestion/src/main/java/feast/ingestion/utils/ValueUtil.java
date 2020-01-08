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

import feast.types.ValueProto.Value;

/**
 * Utility class for converting {@link Value} of different types to a string for storing as key in
 * data stores
 */
public class ValueUtil {

  public static String toString(Value value) {
    String strValue;
    switch (value.getValCase()) {
      case BYTES_VAL:
        strValue = value.getBytesVal().toString();
        break;
      case STRING_VAL:
        strValue = value.getStringVal();
        break;
      case INT32_VAL:
        strValue = String.valueOf(value.getInt32Val());
        break;
      case INT64_VAL:
        strValue = String.valueOf(value.getInt64Val());
        break;
      case DOUBLE_VAL:
        strValue = String.valueOf(value.getDoubleVal());
        break;
      case FLOAT_VAL:
        strValue = String.valueOf(value.getFloatVal());
        break;
      case BOOL_VAL:
        strValue = String.valueOf(value.getBoolVal());
        break;
      default:
        throw new IllegalArgumentException(
            String.format("toString method not supported for type %s", value.getValCase()));
    }
    return strValue;
  }
}
