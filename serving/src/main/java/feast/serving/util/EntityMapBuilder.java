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

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.model.FeatureValue;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/** Utility class for converting response from Feature Storage into {@link Map<String,Entity>}. */
public class EntityMapBuilder {

  // for sorting based on timestamp, newest first.
  private static final Comparator<FeatureValue> TIMESTAMP_COMPARATOR =
      new FeatureValueTimestampComparator().reversed();

  private final Map<String, Map<String, SortedSet<FeatureValue>>> backingMap =
      new ConcurrentHashMap<>();

  public void addFeatureValueList(List<FeatureValue> features) {
    for (FeatureValue feature : features) {
      String entityId = feature.getEntityId();
      String featureId = feature.getFeatureId();

      Map<String, SortedSet<FeatureValue>> featureIdToFeatureValueMap =
          backingMap.computeIfAbsent(entityId, k -> new ConcurrentHashMap<>());

      SortedSet<FeatureValue> featureValues =
          featureIdToFeatureValueMap.computeIfAbsent(
              featureId, k -> new ConcurrentSkipListSet<>(TIMESTAMP_COMPARATOR));
      featureValues.add(feature);
    }
  }

  /**
   * Build an entity map out of {@code this}.
   *
   * @return Entity Map
   */
  public Map<String, Entity> toEntityMap() {
    Map<String, Entity> resultMap = new HashMap<>();
    for (Map.Entry<String, Map<String, SortedSet<FeatureValue>>> entity : backingMap.entrySet()) {
      String entityId = entity.getKey();
      Entity.Builder entityBuilder = Entity.newBuilder();
      for (Map.Entry<String, SortedSet<FeatureValue>> feature : entity.getValue().entrySet()) {
        String featureId = feature.getKey();
        FeatureValueListBuilder featureBuilder = new FeatureValueListBuilder();
        for (FeatureValue featureValue : feature.getValue()) {
          featureBuilder.addValue(featureValue.getValue(), featureValue.getTimestamp());
        }
        entityBuilder.putFeatures(featureId, featureBuilder.build());
      }
      resultMap.put(entityId, entityBuilder.build());
    }
    return resultMap;
  }

  private static class FeatureValueTimestampComparator implements Comparator<FeatureValue> {
    @Override
    public int compare(FeatureValue feature1, FeatureValue feature2) {
      Timestamp t1 = feature1.getTimestamp();
      Timestamp t2 = feature2.getTimestamp();
      return Timestamps.compare(t1, t2);
    }
  }
}
