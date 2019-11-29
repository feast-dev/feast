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
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.storage.RedisProto.RedisKey;
import feast.storage.RedisProto.RedisKey.Builder;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

public class FeatureRowToRedisMutationDoFn extends DoFn<FeatureRow, RedisMutation> {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(FeatureRowToRedisMutationDoFn.class);
  private Map<String, FeatureSetSpec> featureSetSpecs;

  public FeatureRowToRedisMutationDoFn(Map<String, FeatureSetSpec> featureSetSpecs) {
    this.featureSetSpecs = featureSetSpecs;
  }

  private RedisKey getKey(FeatureRow featureRow) {
    FeatureSetSpec featureSetSpec = featureSetSpecs.get(featureRow.getFeatureSet());
    Set<String> entityNames =
        featureSetSpec.getEntitiesList().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toSet());

    Builder redisKeyBuilder = RedisKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
    for (Field field : featureRow.getFieldsList()) {
      if (entityNames.contains(field.getName())) {
        redisKeyBuilder.addEntities(field);
      }
    }
    return redisKeyBuilder.build();
  }

  /** Output a redis mutation object for every feature in the feature row. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRow featureRow = context.element();
    try {
      RedisKey key = getKey(featureRow);
      RedisMutation redisMutation =
          new RedisMutation(Method.SET, key.toByteArray(), featureRow.toByteArray(), null, null);
      context.output(redisMutation);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
