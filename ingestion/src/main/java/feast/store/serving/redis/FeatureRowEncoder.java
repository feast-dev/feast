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
package feast.store.serving.redis;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureRowEncoder {

  private final FeatureSetSpec spec;

  public FeatureRowEncoder(FeatureSetSpec spec) {
    this.spec = spec;
  }

  /**
   * Encode a feature row based on the supplied feature set spec. Entities will be removed, keeping
   * only feature fields without the field names. The field list will be sorted alphabetically based
   * on the feature name. Throw IllegalStateException when there are multiple fields with the same
   * field name but different values.
   *
   * @param featureRow Original feature row
   * @return Encoded feature row based on the criteria above.
   */
  public FeatureRow encode(FeatureRow featureRow) {
    Map<String, Field> fieldValueOnlyMap =
        featureRow.getFieldsList().stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Field::getName,
                    field -> Field.newBuilder().setValue(field.getValue()).build()));

    List<Field> values =
        spec.getFeaturesList().stream()
            .sorted(Comparator.comparing(FeatureSpec::getName))
            .map(
                feature ->
                    fieldValueOnlyMap.getOrDefault(
                        feature.getName(),
                        Field.newBuilder().setValue(Value.getDefaultInstance()).build()))
            .collect(Collectors.toList());

    return FeatureRow.newBuilder()
        .setEventTimestamp(featureRow.getEventTimestamp())
        .addAllFields(values)
        .build();
  }
}
