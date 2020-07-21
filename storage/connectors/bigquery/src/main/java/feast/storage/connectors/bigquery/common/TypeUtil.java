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
import com.google.protobuf.ByteString;
import feast.proto.types.ValueProto;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
   * Converts {@link feast.proto.types.ValueProto.ValueType} to its corresponding {@link
   * StandardSQLTypeName}
   *
   * @param valueType value type to convert
   * @return {@link StandardSQLTypeName}
   */
  public static StandardSQLTypeName toStandardSqlType(ValueProto.ValueType.Enum valueType) {
    return VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(valueType);
  }

  public static Object protoValueToObject(
      ValueProto.Value value, ValueProto.Value.ValCase valCase) {
    switch (valCase) {
      case BYTES_VAL:
        return value.getBytesVal().toByteArray();
      case STRING_VAL:
        return value.getStringVal();
      case INT32_VAL:
        return value.getInt32Val();
      case INT64_VAL:
        return value.getInt64Val();
      case DOUBLE_VAL:
        return value.getDoubleVal();
      case FLOAT_VAL:
        return value.getFloatVal();
      case BOOL_VAL:
        return value.getBoolVal();
      case BYTES_LIST_VAL:
        return value.getBytesListVal().getValList().stream()
            .map(ByteString::toByteArray)
            .collect(Collectors.toList());
      case STRING_LIST_VAL:
        return value.getStringListVal().getValList();
      case INT32_LIST_VAL:
        return value.getInt32ListVal().getValList();
      case INT64_LIST_VAL:
        return value.getInt64ListVal().getValList();
      case DOUBLE_LIST_VAL:
        return value.getDoubleListVal().getValList();
      case FLOAT_LIST_VAL:
        return value.getFloatListVal().getValList();
      case BOOL_LIST_VAL:
        return value.getBoolListVal().getValList();
      case VAL_NOT_SET:
        break;
    }
    return null;
  }

  public static ValueProto.Value objectToProtoValue(
      Object value, ValueProto.Value.ValCase valCase) {
    ValueProto.Value.Builder builder = ValueProto.Value.newBuilder();
    switch (valCase) {
      case BYTES_VAL:
        return builder.setBytesVal(ByteString.copyFrom((byte[]) value)).build();
      case STRING_VAL:
        return builder.setStringVal((String) value).build();
      case INT32_VAL:
        return builder.setInt32Val((Integer) value).build();
      case INT64_VAL:
        return builder.setInt64Val((Long) value).build();
      case DOUBLE_VAL:
        return builder.setDoubleVal((Double) value).build();
      case FLOAT_VAL:
        return builder.setFloatVal((Float) value).build();
      case BOOL_VAL:
        return builder.setBoolVal((Boolean) value).build();
      case BYTES_LIST_VAL:
        return builder
            .setBytesListVal(
                ValueProto.BytesList.newBuilder()
                    .addAllVal(
                        ((List<byte[]>) value)
                            .stream().map(ByteString::copyFrom).collect(Collectors.toList()))
                    .build())
            .build();
      case STRING_LIST_VAL:
        return builder
            .setStringListVal(
                ValueProto.StringList.newBuilder().addAllVal((List<String>) value).build())
            .build();
      case INT32_LIST_VAL:
        return builder
            .setInt32ListVal(
                ValueProto.Int32List.newBuilder().addAllVal((List<Integer>) value).build())
            .build();
      case INT64_LIST_VAL:
        return builder
            .setInt64ListVal(
                ValueProto.Int64List.newBuilder().addAllVal((List<Long>) value).build())
            .build();
      case DOUBLE_LIST_VAL:
        return builder
            .setDoubleListVal(
                ValueProto.DoubleList.newBuilder().addAllVal((List<Double>) value).build())
            .build();
      case FLOAT_LIST_VAL:
        return builder
            .setFloatListVal(
                ValueProto.FloatList.newBuilder().addAllVal((List<Float>) value).build())
            .build();
      case BOOL_LIST_VAL:
        return builder
            .setBoolListVal(
                ValueProto.BoolList.newBuilder().addAllVal((List<Boolean>) value).build())
            .build();
      case VAL_NOT_SET:
        break;
    }
    return null;
  }

  public static Object protoValueToObject(ValueProto.Value value) {
    return protoValueToObject(value, value.getValCase());
  }

  public static Object getDefaultProtoValue(ValueProto.Value.ValCase valCase) {
    return protoValueToObject(ValueProto.Value.getDefaultInstance(), valCase);
  }
}
