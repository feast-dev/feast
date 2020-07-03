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
package feast.storage.connectors.bigtable.retriever;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FeatureRowDecoder {

  private static final String METADATA_CF = "metadata";
  private static final String FEATURES_CF = "features";
  private static final String TIMESTAMP_QUALIFIER = "event_timestamp";
  private final String featureSetRef;
  private final FeatureSetSpec spec;
  private final Set<String> featureRequestNameSet;

  public FeatureRowDecoder(
      String featureSetRef,
      FeatureSetSpec spec,
      ImmutableList<ServingAPIProto.FeatureReference> featureRequestList) {
    this.featureSetRef = featureSetRef;
    this.spec = spec;
    this.featureRequestNameSet =
        featureRequestList.stream()
            .map(ServingAPIProto.FeatureReference::getName)
            .collect(Collectors.toSet());
  }

  /**
   * Decoding feature row by repopulating the field names based on the corresponding feature set
   * spec.
   *
   * @param bigtableFeatureRow Feature row
   * @return boolean
   */
  public FeatureRow decode(Row bigtableFeatureRow) throws InvalidProtocolBufferException {
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
                  List<RowCell> featureValue =
                      bigtableFeatureRow.getCells(FEATURES_CF, featureName);
                  Field.Builder finalField = Field.newBuilder();
                  if (featureValue.isEmpty() || !featureRequestNameSet.contains(featureName)) {
                    finalField.setName(featureName).build();
                  } else {
                    try {
                      finalField
                          .setName(featureName)
                          .setValue(
                              ValueProto.Value.parseFrom(
                                  featureValue.get(0).getValue().toByteArray()));
                    } catch (InvalidProtocolBufferException e) {
                      e.printStackTrace();
                    }
                  }
                  return finalField.build();
                })
            .collect(Collectors.toList());
    byte[] timestamp =
        bigtableFeatureRow
            .getCells(METADATA_CF, TIMESTAMP_QUALIFIER)
            .get(0)
            .getValue()
            .toByteArray();
    return FeatureRow.newBuilder()
        .setFeatureSet(featureSetRef)
        .setEventTimestamp(Timestamp.parseFrom(timestamp))
        .addAllFields(fields)
        .build();
  }
}
