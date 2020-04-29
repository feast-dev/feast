/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.validators;

import feast.core.FeatureSetProto;
import feast.types.ValueProto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FeatureSetValidatorTest {

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowExceptionForFeatureLabelsWithAnEmptyKey() {
    Map<String, String> featureLabels =
        new HashMap<>() {
          {
            put("", "empty_key");
          }
        };

    List<FeatureSetProto.FeatureSpec> featureSpecs = new ArrayList<>();
    featureSpecs.add(
        FeatureSetProto.FeatureSpec.newBuilder()
            .setName("feature1")
            .setValueType(ValueProto.ValueType.Enum.INT64)
            .putAllLabels(featureLabels)
            .build());

    FeatureSetProto.FeatureSetSpec featureSetSpec =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("featureSetWithConstraints")
            .addAllFeatures(featureSpecs)
            .build();
    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpec).build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Feature label keys must not be empty");
    FeatureSetValidator.validateSpec(featureSet);
  }

  @Test
  public void shouldThrowExceptionForFeatureSetLabelsWithAnEmptyKey() {

    Map<String, String> featureSetLabels =
        new HashMap<>() {
          {
            put("", "empty_key");
          }
        };

    FeatureSetProto.FeatureSetSpec featureSetSpec =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("featureSetWithConstraints")
            .putAllLabels(featureSetLabels)
            .build();
    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpec).build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Feature set label keys must not be empty");
    FeatureSetValidator.validateSpec(featureSet);
  }
}
