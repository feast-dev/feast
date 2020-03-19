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
package feast.storage.connectors.redis.retrieval;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FeatureRowDecoder {

  private final String featureSetRef;
  private final FeatureSetSpec spec;

  public FeatureRowDecoder(String featureSetRef, FeatureSetSpec spec) {
    this.featureSetRef = featureSetRef;
    this.spec = spec;
  }

  /**
   * A feature row is considered encoded if the feature set and field names are not set. This method
   * is required for backward compatibility purposes, to allow Feast serving to continue serving non
   * encoded Feature Row ingested by an older version of Feast.
   *
   * @param featureRow Feature row
   * @return boolean
   */
  public Boolean isEncoded(FeatureRow featureRow) {
    return featureRow.getFeatureSet().isEmpty()
        && featureRow.getFieldsList().stream().allMatch(field -> field.getName().isEmpty());
  }

  /**
   * Validates if an encoded feature row can be decoded without exception.
   *
   * @param featureRow Feature row
   * @return boolean
   */
  public Boolean isEncodingValid(FeatureRow featureRow) {
    return featureRow.getFieldsList().size() == spec.getFeaturesList().size();
  }

  /**
   * Decoding feature row by repopulating the field names based on the corresponding feature set
   * spec.
   *
   * @param encodedFeatureRow Feature row
   * @return boolean
   */
  public FeatureRow decode(FeatureRow encodedFeatureRow) {
    final List<Field> fieldsWithoutName = encodedFeatureRow.getFieldsList();

    List<String> featureNames =
        spec.getFeaturesList().stream()
            .sorted(Comparator.comparing(FeatureSpec::getName))
            .map(FeatureSpec::getName)
            .collect(Collectors.toList());
    List<Field> fields =
        IntStream.range(0, featureNames.size())
            .mapToObj(
                featureNameIndex -> {
                  String featureName = featureNames.get(featureNameIndex);
                  return fieldsWithoutName
                      .get(featureNameIndex)
                      .toBuilder()
                      .setName(featureName)
                      .build();
                })
            .collect(Collectors.toList());
    return encodedFeatureRow
        .toBuilder()
        .clearFields()
        .setFeatureSet(featureSetRef)
        .addAllFields(fields)
        .build();
  }
}
