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
import static org.junit.Assert.assertThat;

import com.google.protobuf.Timestamp;
import feast.serving.model.FeatureValue;
import feast.specs.FeatureSpecProto.FeatureSpec;
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
   * @param timestamp event timestamp of data
   */
  public abstract void populate(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      Timestamp timestamp);

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

  public void validate(
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
        Timestamp timestamp = featureValue.getTimestamp();
        validateValue(featureValue, entityId, featureSpec, timestamp);
      }
    }
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
