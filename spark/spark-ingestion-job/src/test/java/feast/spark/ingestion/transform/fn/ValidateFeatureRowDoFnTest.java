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
package feast.spark.ingestion.transform.fn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import feast.ingestion.enums.ValidationStatus;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.proto.core.FeatureSetProto;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.ValueProto;
import feast.spark.ingestion.RowWithValidationResult;
import feast.test.TestUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class ValidateFeatureRowDoFnTest {

  @Test
  public void shouldOutputFailedElementOnFailedValidation() throws Exception {
    FeatureSetProto.FeatureSetSpec fs1 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(ValueProto.ValueType.Enum.INT32)
                    .build())
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature_1")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature_2")
                    .setValueType(ValueProto.ValueType.Enum.INT64)
                    .build())
            .build();

    HashMap<String, FeatureSetProto.FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put("myproject/feature_set", fs1);

    FeatureRowProto.FeatureRow invalidRow =
        FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("invalid").build();

    HashMap<String, feast.ingestion.values.FeatureSet> featureSets =
        featureSetSpecs.entrySet().stream()
            .map(e -> Pair.of(e.getKey(), new feast.ingestion.values.FeatureSet(e.getValue())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (prev, next) -> next, HashMap::new));

    ValidateFeatureRowDoFn validFeat = new ValidateFeatureRowDoFn(featureSets);

    RowWithValidationResult result = validFeat.validateElement(invalidRow);

    assertThat(result.getValidationStatus(), equalTo(ValidationStatus.FAILURE));
  }

  @Test
  public void shouldOutputSuccessStatusOnSuccessfulValidation() throws Exception {
    FeatureSetProto.FeatureSetSpec fs1 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(ValueProto.ValueType.Enum.INT32)
                    .build())
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature_1")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature_2")
                    .setValueType(ValueProto.ValueType.Enum.INT64)
                    .build())
            .build();

    Map<String, FeatureSetProto.FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put("myproject/feature_set", fs1);

    HashMap<String, feast.ingestion.values.FeatureSet> featureSets =
        featureSetSpecs.entrySet().stream()
            .map(e -> Pair.of(e.getKey(), new feast.ingestion.values.FeatureSet(e.getValue())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (prev, next) -> next, HashMap::new));

    ValidateFeatureRowDoFn validFeat = new ValidateFeatureRowDoFn(featureSets);

    FeatureRowProto.FeatureRow randomRow = TestUtil.createRandomFeatureRow(fs1);

    RowWithValidationResult result = validFeat.validateElement(randomRow);

    assertThat(result.getValidationStatus(), equalTo(ValidationStatus.SUCCESS));
  }
}
