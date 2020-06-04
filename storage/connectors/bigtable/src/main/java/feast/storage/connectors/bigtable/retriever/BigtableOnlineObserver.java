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

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FeatureRowProto.FeatureRow.Builder;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BigtableOnlineObserver implements ResponseObserver<Row> {

  private final List<BigtableKey> bigtableKeys;
  private final FeatureSetSpec featureSetSpec;
  private final List<FeatureReference> featureReferences;
  private final ByteString writeTime;
  private final ByteString prefix;
  private final ByteString feature;
  private final ByteString version;
  private HashMap<ByteString, Builder> resultMap;
  private HashMap<ByteString, Descriptor> featureMap;
  private List<FeatureRow> result;

  private BigtableOnlineObserver(
      List<BigtableKey> bigtableKeys,
      FeatureSetSpec featureSetSpec,
      List<FeatureReference> featureReferences,
      String version) {
    this.bigtableKeys = bigtableKeys;
    this.featureSetSpec = featureSetSpec;
    this.featureReferences = featureReferences;
    this.resultMap = new HashMap<>();
    this.featureMap = new HashMap<>();
    this.writeTime = ByteString.copyFromUtf8("writetime");
    this.prefix = ByteString.copyFromUtf8("prefix");
    this.feature = ByteString.copyFromUtf8("feature");
    this.version = ByteString.copyFromUtf8(version);
    this.result = new ArrayList<>();
  }

  public static ResponseObserver create(
      List<BigtableKey> bigtableKeys,
      FeatureSetSpec featureSetSpec,
      List<FeatureReference> featureReferences) {
    return new BigtableOnlineObserver(bigtableKeys, featureSetSpec, featureReferences, "latest");
  }

  @Override
  public void onStart(StreamController streamController) {
    List<FeatureSpec> featuresList = this.featureSetSpec.getFeaturesList();
    HashMap<String, Descriptor> allFeatureMap = new HashMap<>();
    for (FeatureSpec spec : featuresList) {
      allFeatureMap.put(spec.getName(), spec.getDescriptorForType());
    }
    Builder nullFeatureRowBuilder =
        FeatureRow.newBuilder().setFeatureSet(generateFeatureSetStringRef(featureSetSpec));

    for (FeatureReference featureReference : featureReferences) {
      nullFeatureRowBuilder.addFields(Field.newBuilder().setName(featureReference.getName()));
      this.featureMap.put(
          ByteString.copyFromUtf8(featureReference.getName()),
          allFeatureMap.get(featureReference.getName()));
    }
    for (BigtableKey bigtableKey : bigtableKeys) {
      this.resultMap.put(bigtableKey.toByteString(), nullFeatureRowBuilder);
    }
  }

  @Override
  public void onResponse(Row row) {
    List<RowCell> metadata = row.getCells("metadata");
    ByteString cellWriteTime = ByteString.EMPTY;
    ByteString cellPrefix = ByteString.EMPTY;
    ByteString cellFeature = ByteString.EMPTY;
    for (RowCell rowCell : metadata) {
      if (rowCell.getQualifier() == writeTime) {
        cellWriteTime = rowCell.getValue();
      } else if (rowCell.getQualifier() == prefix) {
        cellPrefix = rowCell.getValue();
      } else if (rowCell.getQualifier() == feature) {
        cellFeature = rowCell.getValue();
      }
    }
    if (cellPrefix.isEmpty() || !resultMap.containsKey(cellPrefix) || cellFeature.isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Cell missing important metadata such as prefix or feature name.")
          .asRuntimeException();
    } else {
      Builder resultBuilder = resultMap.get(cellPrefix);
      for (RowCell rowValues : row.getCells("value")) {
        if (rowValues.getQualifier() == version) {
          resultBuilder.addFields(
              Field.newBuilder()
                  .setName(cellFeature.toStringUtf8())
                  .setValue(
                      Value.newBuilder()
                          .setField(
                              featureMap
                                  .get(cellFeature)
                                  .findFieldByName(cellFeature.toStringUtf8()),
                              rowValues.getValue())
                          .build())
                  .build());
        }
      }
    }
  }

  @Override
  public void onError(Throwable throwable) {
    throw Status.INTERNAL
        .withDescription("Encountered an error in a bigtable query")
        .withCause(throwable)
        .asRuntimeException();
  }

  @Override
  public void onComplete() {
    for (BigtableKey key : bigtableKeys) {
      if (!resultMap.containsKey(key)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Somehow a bigtable key did not receive a builder in the resultMap")
            .asRuntimeException();
      } else {
        FeatureRow finalRow = resultMap.get(key).build();
        result.add(finalRow);
      }
    }
  }

  private static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    String ref = String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
    return ref;
  }
}
