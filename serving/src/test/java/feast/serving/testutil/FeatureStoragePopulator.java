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

import static java.util.stream.Collectors.groupingBy;
import static org.apache.hadoop.hbase.shaded.org.junit.Assert.assertNotNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.model.FeatureValue;
import feast.serving.model.Pair;
import feast.serving.util.TimeUtil;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.GranularityProto.Granularity.Enum;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class FeatureStoragePopulator {

  /**
   * Populate feature storage with fake data.
   *
   * @param entityName entity name.
   * @param entityIds collection of entity ID to be added.
   * @param featureSpecs collection of feature's spec for which the data should be added.
   * @param start oldest timestamp of data that should be added. (exclusive)
   * @param end newest timestamp of data that should be added. (inclusive)
   */
  public abstract void populate(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      Timestamp start,
      Timestamp end);

  /**
   * Create a historical feature value based on parameters.
   *
   * @param entityId
   * @param featureId
   * @param ts
   * @param valType
   * @return
   */
  protected Value createValue(
      String entityId, String featureId, Timestamp ts, ValueType.Enum valType) {
    switch (valType) {
      case INT64:
        return Value.newBuilder().setInt64Val(ts.getSeconds()).build();
      case STRING:
        String value = String.format("%s_%s_%s", entityId, featureId, ts.getSeconds());
        return Value.newBuilder().setStringVal(value).build();
      default:
        throw new IllegalArgumentException("not yet supported");
    }
  }

  /**
   * Create single feature value based on parameters.
   *
   * @param entityId
   * @param featureId
   * @param valType
   * @return
   */
  protected Value createValue(String entityId, String featureId, ValueType.Enum valType) {
    switch (valType) {
      case INT64:
        return Value.newBuilder().setInt64Val(Long.parseLong(entityId)).build();
      case STRING:
        String value = String.format("%s_%s", entityId, featureId);
        return Value.newBuilder().setStringVal(value).build();
      default:
        throw new IllegalArgumentException("not yet supported");
    }
  }

  /**
   * Get the duration between 2 fake data based on granularity of a feature.
   *
   * @param granularity feature's granualarity.
   * @return duration between two fake time-series data.
   */
  protected Duration getTimestep(Enum granularity) {
    switch (granularity) {
      case SECOND:
        return Durations.fromSeconds(30);
      case MINUTE:
        return Durations.fromSeconds(60);
      case HOUR:
        return Durations.fromSeconds(60 * 60);
      case DAY:
        return Durations.fromSeconds(24 * 60 * 60);
      default:
        throw new IllegalArgumentException("unsupported granularity: " + granularity);
    }
  }

  public void validateCurrentValueGranularityNone(
      List<FeatureValue> result, List<String> entityIds, List<FeatureSpec> featureSpecs) {
    Map<String, Map<String, List<FeatureValue>>> entityMap = toEntityMap(result);

    assertNotNull(entityMap);
    assertThat(entityMap.size(), equalTo(entityIds.size()));
    for (String entityId : entityIds) {
      Map<String, List<FeatureValue>> featureMap = entityMap.get(entityId);
      assertNotNull(featureMap);
      assertThat(featureMap.size(), equalTo(featureSpecs.size()));
      for (FeatureSpec featureSpec : featureSpecs) {
        List<FeatureValue> featureValueList = featureMap.get(featureSpec.getId());
        assertNotNull(featureValueList);
        assertThat(featureValueList.size(), equalTo(1));

        FeatureValue featureValue = featureValueList.get(0);
        validateValue(featureValue, entityId, featureSpec);
      }
    }
  }

  public void validateCurrentValueOtherGranularity(
      List<FeatureValue> result,
      List<String> entityIds,
      List<FeatureSpec> featureSpecs,
      Timestamp lastTimestamp) {
    Map<String, Map<String, List<FeatureValue>>> entityMap = toEntityMap(result);

    assertNotNull(entityMap);
    assertThat(entityMap.size(), equalTo(entityIds.size()));

    for (String entityId : entityIds) {
      Map<String, List<FeatureValue>> featureMap = entityMap.get(entityId);
      assertNotNull(featureMap);
      assertThat(featureMap.size(), equalTo(featureSpecs.size()));
      for (FeatureSpec featureSpec : featureSpecs) {
        List<FeatureValue> featureValueList = featureMap.get(featureSpec.getId());
        assertNotNull(featureValueList);
        assertThat(featureValueList.size(), equalTo(1));

        FeatureValue featureValue = featureValueList.get(0);
        Timestamp timestamp = featureValue.getTimestamp();
        validateValue(featureValue, entityId, featureSpec, timestamp);
        lastTimestamp = TimeUtil.roundFloorTimestamp(lastTimestamp, featureSpec.getGranularity());
        assertThat(timestamp, equalTo(lastTimestamp));
      }
    }
  }

  public void validateValueWithinTimerange(
      List<FeatureValue> result,
      List<String> entityIds,
      List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs,
      TimestampRange tsRange) {
    Map<String, Map<String, List<FeatureValue>>> entityMap = toEntityMap(result);

    assertNotNull(entityMap);
    assertThat(entityMap.size(), equalTo(entityIds.size()));
    for (String entityId : entityIds) {
      Map<String, List<FeatureValue>> featureMap = entityMap.get(entityId);
      assertThat(featureMap.size(), equalTo(featureSpecLimitPairs.size()));
      for (Pair<FeatureSpec, Integer> featureSpecLimitPair : featureSpecLimitPairs) {
        FeatureSpec featureSpec = featureSpecLimitPair.getLeft();
        int limit = featureSpecLimitPair.getRight();
        List<FeatureValue> featureValues = featureMap.get(featureSpec.getId());
        assertThat(featureValues.size(), lessThanOrEqualTo(limit));

        for (FeatureValue featureValue : featureValues) {
          Timestamp timestamp = featureValue.getTimestamp();
          validateValue(featureValue, entityId, featureSpec, timestamp);

          // check timestamp is within specified range
          assertThat(Timestamps.compare(timestamp, tsRange.getEnd()), lessThanOrEqualTo(0));
          assertThat(Timestamps.compare(timestamp, tsRange.getStart()), greaterThanOrEqualTo(0));
        }
      }
    }
  }

  private void validateValue(FeatureValue featureValue, String entityId, FeatureSpec featureSpec) {
    Value actualValue = featureValue.getValue();
    Value expectedValue = createValue(entityId, featureSpec.getId(), featureSpec.getValueType());
    assertThat(actualValue, equalTo(expectedValue));
  }

  private void validateValue(
      FeatureValue featureValue, String entityId, FeatureSpec featureSpec, Timestamp timestamp) {
    Value actualValue = featureValue.getValue();
    Value expectedValue =
        createValue(entityId, featureSpec.getId(), timestamp, featureSpec.getValueType());
    assertThat(actualValue, equalTo(expectedValue));
  }

  private Map<String, Map<String, List<FeatureValue>>> toEntityMap(
      List<FeatureValue> featureValues) {
    Map<String, List<FeatureValue>> temp =
        featureValues.stream().collect(groupingBy(FeatureValue::getEntityId));

    Map<String, Map<String, List<FeatureValue>>> entityMap = new HashMap<>();
    for (Map.Entry<String, List<FeatureValue>> entry : temp.entrySet()) {
      entityMap.put(
          entry.getKey(),
          entry.getValue().stream().collect(groupingBy(FeatureValue::getFeatureId)));
    }
    return entityMap;
  }
}
