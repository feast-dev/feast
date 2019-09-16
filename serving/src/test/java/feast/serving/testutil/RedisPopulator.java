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

package feast.serving.testutil;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureProto.Field;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Jedis;

public class RedisPopulator extends FeatureStoragePopulator {

  private final Jedis jedis;

  public RedisPopulator(String redisHost, int redisPort) {
    jedis = new Jedis(redisHost, redisPort);
  }

  @Override
  public void populate(List<Field> fields, FeatureSetSpec featureSetSpec, FeatureRow featureRow) {
    // Get a list of entity names
    List<String> entityNames = new ArrayList<>();
    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      entityNames.add(entitySpec.getName());
    }

    // Construct key
    RedisKey.Builder redisKeyBuilder = RedisKey.newBuilder()
        .setFeatureSet(featureRow.getFeatureSet());

    for (Field field : fields) {
      // Check if name is in EntitySpec
      if (entityNames.stream().anyMatch(entityName -> entityName.trim().equals(field.getName()))) {
        redisKeyBuilder.addEntities(field);
      }
    }

    jedis.set(redisKeyBuilder.build().toByteArray(), featureRow.toByteArray());
  }

}

