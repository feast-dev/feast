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

package feast.serving.util;

import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.model.FeatureValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Utility class for converting response from Feature Storage into {@link Map<String,Entity>}. */
public class EntityMapBuilder {

  private final Map<String, Map<String, FeatureValue>> backingMap = new ConcurrentHashMap<>();

  public void addFeatureValueList(List<FeatureValue> features) {
    for (FeatureValue feature : features) {
      String entityId = feature.getEntityId();
      String featureId = feature.getFeatureId();

      Map<String, FeatureValue> featureIdToFeatureValueMap =
          backingMap.computeIfAbsent(entityId, k -> new ConcurrentHashMap<>());

      featureIdToFeatureValueMap.put(featureId, feature);
    }
  }

  /**
   * Build an entity map out of {@code this}.
   *
   * @return Entity Map
   */
  public Map<String, Entity> toEntityMap() {
    Map<String, Entity> resultMap = new HashMap<>();
    for (Map.Entry<String, Map<String, FeatureValue>> entity : backingMap.entrySet()) {
      String entityId = entity.getKey();
      Entity.Builder entityBuilder = Entity.newBuilder();
      for (Map.Entry<String, FeatureValue> feature : entity.getValue().entrySet()) {
        String featureId = feature.getKey();
        FeatureValue featureValue = feature.getValue();
        ServingAPIProto.FeatureValue featureValueProto = ServingAPIProto.FeatureValue.newBuilder()
            .setTimestamp(featureValue.getTimestamp())
            .setValue(featureValue.getValue())
            .build();

        entityBuilder.putFeatures(featureId, featureValueProto);
      }
      resultMap.put(entityId, entityBuilder.build());
    }
    return resultMap;
  }
}
