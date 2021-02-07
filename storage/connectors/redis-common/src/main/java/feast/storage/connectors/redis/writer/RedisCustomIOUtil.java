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
package feast.storage.connectors.redis.writer;

import com.google.common.collect.Streams;
import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.FeatureSetProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class RedisCustomIOUtil {

  public static RedisProto.RedisKey getKey(
      FeatureRowProto.FeatureRow featureRow, FeatureSetProto.FeatureSetSpec spec) {
    List<String> entityNames =
        spec.getEntitiesList().stream()
            .map(FeatureSetProto.EntitySpec::getName)
            .sorted()
            .collect(Collectors.toList());

    Map<String, FieldProto.Field> entityFields = new HashMap<>();
    RedisProto.RedisKey.Builder redisKeyBuilder =
        RedisProto.RedisKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
    for (FieldProto.Field field : featureRow.getFieldsList()) {
      if (entityNames.contains(field.getName())) {
        entityFields.putIfAbsent(
            field.getName(),
            FieldProto.Field.newBuilder()
                .setName(field.getName())
                .setValue(field.getValue())
                .build());
      }
    }
    for (String entityName : entityNames) {
      redisKeyBuilder.addEntities(entityFields.get(entityName));
    }
    return redisKeyBuilder.build();
  }

  /**
   * Encode the Feature Row as bytes to store in Redis in encoded Feature Row encoding. To reduce
   * storage space consumption in redis, feature rows are "encoded" by hashing the fields names and
   * not unsetting the feature set reference. FeatureRowDecoder is responsible for reversing this
   * "encoding" step.
   */
  public static FeatureRowProto.FeatureRow getValue(
      FeatureRowProto.FeatureRow featureRow, FeatureSetProto.FeatureSetSpec spec) {
    List<String> featureNames =
        spec.getFeaturesList().stream()
            .map(FeatureSetProto.FeatureSpec::getName)
            .collect(Collectors.toList());

    Map<String, FieldProto.Field.Builder> fieldValueOnlyMap =
        featureRow.getFieldsList().stream()
            .filter(field -> featureNames.contains(field.getName()))
            .distinct()
            .collect(
                Collectors.toMap(
                    FieldProto.Field::getName,
                    field -> FieldProto.Field.newBuilder().setValue(field.getValue())));

    List<FieldProto.Field> values =
        featureNames.stream()
            .sorted()
            .map(
                featureName -> {
                  FieldProto.Field.Builder field =
                      fieldValueOnlyMap.getOrDefault(
                          featureName,
                          FieldProto.Field.newBuilder()
                              .setValue(ValueProto.Value.getDefaultInstance()));

                  // Encode the name of the as the hash of the field name.
                  // Use hash of name instead of the name of to reduce redis storage consumption
                  // per feature row stored.
                  String nameHash =
                      Hashing.murmur3_32()
                          .hashString(featureName, StandardCharsets.UTF_8)
                          .toString();
                  field.setName(nameHash);

                  return field.build();
                })
            .collect(Collectors.toList());

    return FeatureRowProto.FeatureRow.newBuilder()
        .setEventTimestamp(featureRow.getEventTimestamp())
        .addAllFields(values)
        .build();
  }

  public static boolean rowShouldBeWritten(FeatureRowProto.FeatureRow newRow, byte[] currentValue) {
    if (currentValue == null) {
      // nothing to compare with
      return true;
    }
    FeatureRowProto.FeatureRow currentRow;
    try {
      currentRow = FeatureRowProto.FeatureRow.parseFrom(currentValue);
    } catch (InvalidProtocolBufferException e) {
      // definitely need to replace current value
      return true;
    }

    // check whether new row has later eventTimestamp
    return newRow.getEventTimestamp().getSeconds() > currentRow.getEventTimestamp().getSeconds();
  }

  /** Deduplicate rows by key within batch. Keep only latest eventTimestamp */
  public static Map<RedisProto.RedisKey, FeatureRowProto.FeatureRow> deduplicateRows(
      Iterable<FeatureRowProto.FeatureRow> rows,
      Map<String, FeatureSetProto.FeatureSetSpec> latestSpecs) {
    Comparator<FeatureRowProto.FeatureRow> byEventTimestamp =
        Comparator.comparing(r -> r.getEventTimestamp().getSeconds());

    FeatureRowProto.FeatureRow identity =
        FeatureRowProto.FeatureRow.newBuilder()
            .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(-1).build())
            .build();

    return Streams.stream(rows)
        .collect(
            Collectors.groupingBy(
                row -> getKey(row, latestSpecs.get(row.getFeatureSet())),
                Collectors.reducing(identity, BinaryOperator.maxBy(byEventTimestamp))));
  }
}
