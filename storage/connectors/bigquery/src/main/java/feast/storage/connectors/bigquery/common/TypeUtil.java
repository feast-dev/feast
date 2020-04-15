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
package feast.storage.connectors.bigquery.common;

import com.google.cloud.bigquery.StandardSQLTypeName;
import feast.types.ValueProto;
import java.util.HashMap;
import java.util.Map;

public class TypeUtil {

  private static final Map<ValueProto.ValueType.Enum, StandardSQLTypeName>
      VALUE_TYPE_TO_STANDARD_SQL_TYPE = new HashMap<>();

  static {
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueProto.ValueType.Enum.BYTES, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.STRING, StandardSQLTypeName.STRING);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueProto.ValueType.Enum.INT32, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueProto.ValueType.Enum.INT64, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.DOUBLE, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.FLOAT, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueProto.ValueType.Enum.BOOL, StandardSQLTypeName.BOOL);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.BYTES_LIST, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.STRING_LIST, StandardSQLTypeName.STRING);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.INT32_LIST, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.INT64_LIST, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.DOUBLE_LIST, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.FLOAT_LIST, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(
        ValueProto.ValueType.Enum.BOOL_LIST, StandardSQLTypeName.BOOL);
  }

  /**
   * Converts {@link feast.types.ValueProto.ValueType} to its corresponding {@link
   * StandardSQLTypeName}
   *
   * @param valueType value type to convert
   * @return {@link StandardSQLTypeName}
   */
  public static StandardSQLTypeName toStandardSqlType(ValueProto.ValueType.Enum valueType) {
    return VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(valueType);
  }
}
