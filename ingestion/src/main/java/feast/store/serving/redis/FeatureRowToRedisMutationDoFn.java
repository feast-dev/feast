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
package feast.store.serving.redis;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.storage.RedisProto.RedisKey;
import feast.storage.RedisProto.RedisKey.Builder;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

public class FeatureRowToRedisMutationDoFn extends DoFn<FeatureRow, RedisMutation> {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(FeatureRowToRedisMutationDoFn.class);
  private Map<String, FeatureSet> featureSets;

  public FeatureRowToRedisMutationDoFn(Map<String, FeatureSet> featureSets) {
    this.featureSets = featureSets;
  }

  private RedisKey getKey(FeatureRow featureRow) {
    FeatureSet featureSet = featureSets.get(featureRow.getFeatureSet());
    List<String> entityNames =
        featureSet.getSpec().getEntitiesList().stream()
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
    return redisKeyBuilder.build();
  }

  private byte[] getValue(FeatureRow featureRow) {
    FeatureSetSpec spec = featureSets.get(featureRow.getFeatureSet()).getSpec();

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
                        Field.newBuilder().setValue(ValueProto.Value.getDefaultInstance()).build()))
            .collect(Collectors.toList());

    return FeatureRow.newBuilder()
        .setEventTimestamp(featureRow.getEventTimestamp())
        .addAllFields(values)
        .build()
        .toByteArray();
  }

  /** Output a redis mutation object for every feature in the feature row. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRow featureRow = context.element();
    try {
      byte[] key = getKey(featureRow).toByteArray();
      byte[] value = getValue(featureRow);
      RedisMutation redisMutation = new RedisMutation(Method.SET, key, value, null, null);
      context.output(redisMutation);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
