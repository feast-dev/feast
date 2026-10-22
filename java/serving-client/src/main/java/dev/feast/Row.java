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
package dev.feast;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.Value.ValCase;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("UnusedReturnValue")
public class Row {
  private Timestamp entity_timestamp;
  private HashMap<String, Value> fields;
  private HashMap<String, FieldStatus> fieldStatuses;

  public static Row create() {
    Row row = new Row();
    row.entity_timestamp = Timestamps.fromMillis(System.currentTimeMillis());
    row.fields = new HashMap<>();
    row.fieldStatuses = new HashMap<>();
    return row;
  }

  public Row setEntityTimestamp(Instant timestamp) {
    entity_timestamp = Timestamps.fromMillis(timestamp.toEpochMilli());
    return this;
  }

  public Timestamp getEntityTimestamp() {
    return entity_timestamp;
  }

  public Row setEntityTimestamp(String dateTime) {
    entity_timestamp = Timestamps.fromMillis(Instant.parse(dateTime).toEpochMilli());
    return this;
  }

  public Row set(String fieldName, Object value) {
    return this.set(fieldName, value, FieldStatus.PRESENT);
  }

  public Row set(String fieldName, Object value, FieldStatus status) {
    String valueType = value.getClass().getCanonicalName();
    switch (valueType) {
      case "java.lang.Integer":
        fields.put(fieldName, Value.newBuilder().setInt32Val((int) value).build());
        break;
      case "java.lang.Long":
        fields.put(fieldName, Value.newBuilder().setInt64Val((long) value).build());
        break;
      case "java.lang.Float":
        fields.put(fieldName, Value.newBuilder().setFloatVal((float) value).build());
        break;
      case "java.lang.Double":
        fields.put(fieldName, Value.newBuilder().setDoubleVal((double) value).build());
        break;
      case "java.lang.String":
        fields.put(fieldName, Value.newBuilder().setStringVal((String) value).build());
        break;
      case "byte[]":
        fields.put(
            fieldName, Value.newBuilder().setBytesVal(ByteString.copyFrom((byte[]) value)).build());
        break;
      case "feast.proto.types.ValueProto.Value":
        fields.put(fieldName, (Value) value);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Type '%s' is unsupported in Feast. Please use one of these value types: Integer, Long, Float, Double, String, byte[].",
                valueType));
    }

    fieldStatuses.put(fieldName, status);
    return this;
  }

  public Map<String, Value> getFields() {
    return fields;
  }

  public Integer getInt(String fieldName) {
    return getValue(fieldName).map(Value::getInt32Val).orElse(null);
  }

  public Long getLong(String fieldName) {
    return getValue(fieldName).map(Value::getInt64Val).orElse(null);
  }

  public Float getFloat(String fieldName) {
    return getValue(fieldName).map(Value::getFloatVal).orElse(null);
  }

  public Double getDouble(String fieldName) {
    return getValue(fieldName).map(Value::getDoubleVal).orElse(null);
  }

  public String getString(String fieldName) {
    return getValue(fieldName).map(Value::getStringVal).orElse(null);
  }

  public byte[] getByte(String fieldName) {
    return getValue(fieldName).map(Value::getBytesVal).map(ByteString::toByteArray).orElse(null);
  }

  public Map<String, FieldStatus> getStatuses() {
    return fieldStatuses;
  }

  public FieldStatus getStatus(String fieldName) {
    return fieldStatuses.get(fieldName);
  }

  @Override
  public String toString() {
    List<String> parts = new ArrayList<>();
    fields.forEach(
        (key, value) ->
            parts.add(
                key
                    + ":"
                    + (value.getValCase().equals(ValCase.VAL_NOT_SET)
                        ? "NULL"
                        : value.toString().trim())));
    return String.join(", ", parts);
  }

  private Optional<Value> getValue(String fieldName) {
    if (!fields.containsKey(fieldName)) {
      throw new IllegalArgumentException(
          String.format("Row does not contain field '%s'", fieldName));
    }
    Value value = fields.get(fieldName);
    if (value.getValCase().equals(ValCase.VAL_NOT_SET)) {
      return Optional.empty();
    }
    return Optional.of(value);
  }
}
