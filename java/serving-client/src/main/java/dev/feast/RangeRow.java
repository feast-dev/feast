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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("UnusedReturnValue")
public class RangeRow {
  private HashMap<String, Value> entity;
  private List<Timestamp> field_timestamps;
  private HashMap<String, List<Value>> fields;
  private HashMap<String, List<FieldStatus>> fieldStatuses;

  public static RangeRow create() {
    RangeRow row = new RangeRow();
    row.entity = new HashMap<>();
    row.field_timestamps = new ArrayList<>();
    row.fields = new HashMap<>();
    row.fieldStatuses = new HashMap<>();
    return row;
  }

  public RangeRow setEntityTimestamp(List<Instant> timestamps) {
    field_timestamps =
        timestamps.stream()
            .map(t -> Timestamps.fromMillis(t.toEpochMilli()))
            .collect(Collectors.toList());
    return this;
  }

  public List<Timestamp> getEntityTimestamps() {
    return field_timestamps;
  }

  public RangeRow setEntityTimestamps(List<String> dateTimes) {
    field_timestamps =
        dateTimes.stream()
            .map(dt -> Timestamps.fromMillis(Instant.parse(dt).toEpochMilli()))
            .collect(Collectors.toList());
    return this;
  }

  public HashMap<String, Value> getEntity() {
    return entity;
  }

  public RangeRow setEntity(String fieldName, Object value) {
    entity.put(fieldName, RequestUtil.objectToValue(value));
    return this;
  }

  public RangeRow set(String fieldName, List<Object> values, List<FieldStatus> statuses) {
    List<Value> valueList =
        values.stream().map(RequestUtil::objectToValue).collect(Collectors.toList());
    fields.put(fieldName, valueList);
    fieldStatuses.put(fieldName, statuses);
    return this;
  }

  public RangeRow setWithValues(String fieldName, List<Value> values, List<FieldStatus> statuses) {
    fields.put(fieldName, values);
    fieldStatuses.put(fieldName, statuses);
    return this;
  }

  public Map<String, List<Value>> getFields() {
    return fields;
  }

  public List<Integer> getInts(String fieldName) {
    return getValues(fieldName).stream().map(Value::getInt32Val).collect(Collectors.toList());
  }

  public List<Long> getLongs(String fieldName) {
    return getValues(fieldName).stream().map(Value::getInt64Val).collect(Collectors.toList());
  }

  public List<Float> getFloats(String fieldName) {
    return getValues(fieldName).stream().map(Value::getFloatVal).collect(Collectors.toList());
  }

  public List<Double> getDoubles(String fieldName) {
    return getValues(fieldName).stream().map(Value::getDoubleVal).collect(Collectors.toList());
  }

  public List<String> getStrings(String fieldName) {
    return getValues(fieldName).stream().map(Value::getStringVal).collect(Collectors.toList());
  }

  public List<byte[]> getBytes(String fieldName) {
    return getValues(fieldName).stream()
        .map(Value::getBytesVal)
        .map(ByteString::toByteArray)
        .collect(Collectors.toList());
  }

  public List<Boolean> getBooleans(String fieldName) {
    return getValues(fieldName).stream().map(Value::getBoolVal).collect(Collectors.toList());
  }

  public List<List<?>> getLists(String fieldName) {
    return getValues(fieldName).stream()
        .map(
            v -> {
              switch (v.getValCase()) {
                case INT32_LIST_VAL:
                  return v.getInt32ListVal().getValList();
                case INT64_LIST_VAL:
                  return v.getInt64ListVal().getValList();
                case FLOAT_LIST_VAL:
                  return v.getFloatListVal().getValList();
                case DOUBLE_LIST_VAL:
                  return v.getDoubleListVal().getValList();
                case STRING_LIST_VAL:
                  return v.getStringListVal().getValList();
                case BYTES_LIST_VAL:
                  return v.getBytesListVal().getValList().stream()
                      .map(ByteString::toByteArray)
                      .collect(Collectors.toList());
                case BOOL_LIST_VAL:
                  return v.getBoolListVal().getValList();
                default:
                  throw new IllegalArgumentException(
                      String.format("Unsupported list type: %s", v.getValCase().name()));
              }
            })
        .collect(Collectors.toList());
  }

  public Map<String, List<FieldStatus>> getStatuses() {
    return fieldStatuses;
  }

  public List<FieldStatus> getStatus(String fieldName) {
    return fieldStatuses.get(fieldName);
  }

  private List<Value> getValues(String fieldName) {
    if (!fields.containsKey(fieldName)) {
      throw new IllegalArgumentException(
          String.format("RangeRow does not contain field '%s'", fieldName));
    }
    return fields.get(fieldName);
  }
}
