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

import com.google.api.LabelDescriptor.ValueType;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.service.spec.SpecService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeRedisCoreService implements SpecService {

  private static final String REDIS_HOST = "localhost";
  private static final int REDIS_PORT = 6379;

  private static final List<String> ENTITY_NAMES = Arrays
      .asList("test_entity_name_1", "test_entity_name_2", "test_entity_name_3");
  private static final List<String> FEATURE_NAMES = Arrays
      .asList("test_entity_name_1", "test_entity_name_2", "test_entity_name_3");

  @Override
  public Store getStoreDetails(String id) {
    return Store.newBuilder().setName(id).setType(StoreType.REDIS)
        .setRedisConfig(RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build())
        .build();
  }

  @Override
  public Map<String, FeatureSetSpec> getFeatureSetSpecs(List<Subscription> subscriptions) {
    Map<String, FeatureSetSpec> featureSetSpecMap = new HashMap<>();
    // All subscription will have the same entity and feature name of string type
    List<EntitySpec> entitySpecs = new ArrayList<>();
    List<FeatureSpec> featureSpecs = new ArrayList<>();
    for (String entityName : ENTITY_NAMES) {
      entitySpecs.add(EntitySpec.newBuilder().setName(entityName)
          .setValueTypeValue(ValueType.STRING_VALUE).build());
    }
    for (String featureName : FEATURE_NAMES) {
      featureSpecs.add(FeatureSpec.newBuilder().setName(featureName)
          .setValueTypeValue(ValueType.STRING_VALUE).build());
    }
    for (Subscription subscription : subscriptions) {
      FeatureSetSpec featureSetSpec = FeatureSetSpec.newBuilder()
          .setName(subscription.getName()).setVersion(Integer.parseInt(subscription.getVersion()))
          .addAllEntities(entitySpecs)
          .addAllFeatures(featureSpecs).build();

      featureSetSpecMap
          .put(String.format("%s:%s", subscription.getName(), subscription.getVersion()),
              featureSetSpec);
    }
    return featureSetSpecMap;
  }

  @Override
  public boolean isConnected() {
    return true;
  }

}
