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
package feast.storage.connectors.redis.retriever;

import com.google.common.hash.Hashing;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
   * Check if encoded feature row can be decoded by v1 Decoder. The v1 Decoder requires that the
   * Feature Row to have both it's feature set reference and fields names are not set. The no. of
   * fields in the feature row should also match up with the number of fields in the Feature Set
   * spec. NOTE: This method is deprecated and will be removed in Feast v0.7.
   *
   * @param featureRow Feature row
   * @return boolean
   */
  @Deprecated
  private boolean isEncodedV1(FeatureRow featureRow) {
    return featureRow.getFeatureSet().isEmpty()
        && featureRow.getFieldsList().stream().allMatch(field -> field.getName().isEmpty())
        && featureRow.getFieldsList().size() == spec.getFeaturesList().size();
  }

  /**
   * Check if encoded feature row can be decoded by Decoder. The v2 Decoder requires that a Feature
   * Row to have both it feature set reference and fields names are set.
   *
   * @param featureRow Feature row
   * @return boolean
   */
  private boolean isEncodedV2(FeatureRow featureRow) {
    return !featureRow.getFieldsList().stream().anyMatch(field -> field.getName().isEmpty());
  }

  /**
   * Decode feature row encoded by RedisCustomIO. NOTE: The v1 Decoder will be removed in Feast 0.7
   *
   * @throws IllegalArgumentException if unable to the decode the given feature row
   * @param encodedFeatureRow Feature row
   * @return boolean
   */
  public FeatureRow decode(FeatureRow encodedFeatureRow) {
    if (isEncodedV1(encodedFeatureRow)) {
      // TODO: remove v1 feature row decoder in Feast 0.7
      // Decode Feature Rows using the v1 Decoder.
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
    if (isEncodedV2(encodedFeatureRow)) {
      // Decode Feature Rows using the v2 Decoder.
      // v2 Decoder input Feature Rows should use a hashed name as the field name and
      // should not have feature set reference set.
      // Decoding reverts the field name to a unhashed string and set feature set reference.
      Map<String, Value> nameHashValueMap =
          encodedFeatureRow.getFieldsList().stream()
              .collect(Collectors.toMap(field -> field.getName(), field -> field.getValue()));

      List<String> featureNames =
          spec.getFeaturesList().stream().map(FeatureSpec::getName).collect(Collectors.toList());

      List<Field> fields =
          featureNames.stream()
              .map(
                  name -> {
                    String nameHash =
                        Hashing.murmur3_32().hashString(name, StandardCharsets.UTF_8).toString();
                    Value value =
                        nameHashValueMap.getOrDefault(nameHash, Value.newBuilder().build());
                    return Field.newBuilder().setName(name).setValue(value).build();
                  })
              .collect(Collectors.toList());

      return encodedFeatureRow
          .toBuilder()
          .clearFields()
          .setFeatureSet(featureSetRef)
          .addAllFields(fields)
          .build();
    }
    throw new IllegalArgumentException("Failed to decode FeatureRow row: Possible data corruption");
  }
}
