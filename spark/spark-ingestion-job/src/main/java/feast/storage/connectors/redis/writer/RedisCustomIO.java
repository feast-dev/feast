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
package feast.storage.connectors.redis.writer;

import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto.Store.*;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.storage.RedisProto.RedisKey.Builder;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * * Class copied and adapted from feast-storage-connector-redis RedisCustomIO.
 *
 * <p>TODO move common ingestion code into a shared project.
 */
public class RedisCustomIO {

  public static class Write implements Serializable {

    private Map<String, FeatureSetSpec> featureSetSpecs;

    public Write(Map<String, FeatureSetSpec> featureSetSpecs) {
      this.featureSetSpecs = featureSetSpecs;
    }

    public byte[] getKey(FeatureRow featureRow) {
      FeatureSetSpec featureSetSpec = featureSetSpecs.get(featureRow.getFeatureSet());
      if (featureSetSpec == null) {
        return null;
      }
      List<String> entityNames =
          featureSetSpec.getEntitiesList().stream()
              .map(EntitySpec::getName)
              .sorted()
              .collect(Collectors.toList());

      Map<String, Field> entityFields = new HashMap<>();
      Builder redisKeyBuilder = RedisKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
      for (Field field : featureRow.getFieldsList()) {
        if (entityNames.contains(field.getName())) {
          entityFields.putIfAbsent(
              field.getName(),
              Field.newBuilder().setName(field.getName()).setValue(field.getValue()).build());
        }
      }
      for (String entityName : entityNames) {
        redisKeyBuilder.addEntities(entityFields.get(entityName));
      }
      return redisKeyBuilder.build().toByteArray();
    }

    public byte[] getValue(FeatureRow featureRow) {
      FeatureSetSpec spec = featureSetSpecs.get(featureRow.getFeatureSet());

      List<String> featureNames =
          spec.getFeaturesList().stream().map(FeatureSpec::getName).collect(Collectors.toList());
      Map<String, Field> fieldValueOnlyMap =
          featureRow.getFieldsList().stream()
              .filter(field -> featureNames.contains(field.getName()))
              .distinct()
              .collect(
                  Collectors.toMap(
                      Field::getName,
                      field -> Field.newBuilder().setValue(field.getValue()).build()));

      List<Field> values =
          featureNames.stream()
              .sorted()
              .map(
                  featureName ->
                      fieldValueOnlyMap.getOrDefault(
                          featureName,
                          Field.newBuilder()
                              .setValue(ValueProto.Value.getDefaultInstance())
                              .build()))
              .collect(Collectors.toList());

      return FeatureRow.newBuilder()
          .setEventTimestamp(featureRow.getEventTimestamp())
          .addAllFields(values)
          .build()
          .toByteArray();
    }
  }
}
