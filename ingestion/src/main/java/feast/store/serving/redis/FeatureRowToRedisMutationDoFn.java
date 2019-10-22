/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.store.serving.redis;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.storage.RedisProto.RedisKey;
import feast.storage.RedisProto.RedisKey.Builder;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FieldProto.Field;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
@AllArgsConstructor
public class FeatureRowToRedisMutationDoFn extends DoFn<FeatureRow, RedisMutation> {

  private FeatureSetSpec featureSetSpec;

  private RedisKey getKey(FeatureRow featureRow) {
    Set<String> entityNames = featureSetSpec.getEntitiesList().stream()
        .map(EntitySpec::getName).collect(Collectors.toSet());

    Builder redisKeyBuilder = RedisKey.newBuilder()
        .setFeatureSet(featureRow.getFeatureSet());
    for (Field field : featureRow.getFieldsList()) {
      if (entityNames.contains(field.getName())) {
        redisKeyBuilder.addEntities(field);
      }
    }
    return redisKeyBuilder.build();
  }

  /**
   * Output a redis mutation object for every feature in the feature row.
   */
  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRow featureRow = context.element();
    RedisKey key = getKey(featureRow);
    context.output(
        RedisMutation.builder()
            .key(key.toByteArray())
            .value(featureRow.toByteArray())
            .method(Method.SET)
            .build());
  }
}
