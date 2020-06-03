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

import com.google.api.gax.rpc.StreamController;
import com.google.bigtable.v2.Cell;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FeatureRowProto.FeatureRowOrBuilder;
import feast.proto.types.FeatureRowProto.FeatureRow.Builder;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import io.grpc.Status;

import javax.print.DocFlavor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class BigtableOnlineObserver implements ResponseObserver<Row> {

  private final List<BigtableKey> bigtableKeys;
  private final FeatureSetSpec featureSetSpec;
  private final List<FeatureReference> featureReferences;
  private final HashMap<ByteString, Builder> resultMap;
  private final HashMap<ByteString, Value> featureMap;
  private final ByteString writeTime;
  private final ByteString prefix;
  private final ByteString feature;
  private final ByteString version;

  private BigtableOnlineObserver(List<BigtableKey> bigtableKeys, FeatureSetSpec featureSetSpec, List<FeatureReference> featureReferences, String version) {
    this.bigtableKeys = bigtableKeys;
    this.featureSetSpec = featureSetSpec;
    this.featureReferences = featureReferences;
    this.resultMap = new HashMap<>();
    this.featureMap = new HashMap<>();
    this.writeTime = ByteString.copyFromUtf8("writetime");
    this.prefix = ByteString.copyFromUtf8("prefix");
    this.feature = ByteString.copyFromUtf8("feature");
    this.version = ByteString.copyFromUtf8(version);
  }

  public static ResponseObserver create(List<BigtableKey> bigtableKeys, FeatureSetSpec featureSetSpec, List<FeatureReference> featureReferences) {
    return new BigtableOnlineObserver(bigtableKeys, featureSetSpec, featureReferences, "latest");
  }

  @Override
  public void onStart(StreamController streamController) {
    List<FeatureSpec> featuresList = this.featureSetSpec.getFeaturesList();
    HashMap<ByteString, Enum> allFeatureMap = new HashMap<>();
    for (FeatureSpec spec : featuresList) {
      allFeatureMap.put(ByteString.copyFromUtf8(spec.getName()), spec.getValueType());
    }
    Builder nullFeatureRowBuilder =
            FeatureRow.newBuilder().setFeatureSet(generateFeatureSetStringRef(featureSetSpec));
    for (FeatureReference featureReference : featureReferences) {
      nullFeatureRowBuilder.addFields(Field.newBuilder().setName(featureReference.getName()));
      this.featureMap.put(ByteString.copyFromUtf8(featureReference.getName()),
              
    }
    for (BigtableKey bigtableKey: bigtableKeys) {
      this.resultMap.put(bigtableKey.toByteString(), nullFeatureRowBuilder);
    }
  }

  @Override
  public void onResponse(Row row) {
    List<RowCell> metadata = row.getCells("metadata");
    ByteString cellWriteTime = ByteString.EMPTY;
    ByteString cellPrefix = ByteString.EMPTY;
    ByteString cellFeature = ByteString.EMPTY;
    for (RowCell rowCell: metadata) {
      if (rowCell.getQualifier() == writeTime) {
        cellWriteTime = rowCell.getValue();
      } else if (rowCell.getQualifier() == prefix) {
        cellPrefix = rowCell.getValue();
      } else if (rowCell.getQualifier() == feature) {
        cellFeature = rowCell.getValue();
    }
    if (cellPrefix.isEmpty() || !resultMap.containsKey(cellPrefix) || cellFeature.isEmpty()) {
      throw Status.NOT_FOUND
              .withDescription("Unable to retrieve prefix from bigtable")
              .asRuntimeException();
    } else {
      Builder resultBuilder = resultMap.get(cellPrefix);
      for (RowCell rowValues: row.getCells("value")) {
        if (rowValues.getQualifier() == version) {
          resultBuilder.addFields(Field.newBuilder().setName(cellFeature.toStringUtf8()).setValue(Value.newBuilder()..build()).build());
        }
    }
    }

  }

}

  /**
   * Gets online features from bigtable. This method returns a list of {@link FeatureRow}s
   * corresponding to each feature set spec. Each feature row in the list then corresponds to an
   * {@link EntityRow} provided by the user.
   *
   * @param entityRows list of entity rows in the feature request
   * @param featureSetRequests Map of {@link FeatureSetSpec} to
   *     feature references in the request tied to that feature set.
   * @return List of List of {@link FeatureRow}
   */
  @Override
  public List<List<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, List<FeatureSetRequest> featureSetRequests) {

    List<List<FeatureRow>> featureRows = new ArrayList<>();
    for (FeatureSetRequest featureSetRequest : featureSetRequests) {
      List<BigtableKey> bigtableKeys = buildBigtableKeys(entityRows, featureSetRequest.getSpec());
      try {
        List<FeatureRow> featureRowsForFeatureSet =
            sendAndProcessMultiGet(
                bigtableKeys,
                featureSetRequest.getSpec(),
                featureSetRequest.getFeatureReferences().asList());
        featureRows.add(featureRowsForFeatureSet);
      } catch (InvalidProtocolBufferException | ExecutionException e) {
        throw Status.INTERNAL
            .withDescription("Unable to parse protobuf while retrieving feature")
            .withCause(e)
            .asRuntimeException();
      }
    }
    return featureRows;
  }

  private List<BigtableKey> buildBigtableKeys(List<EntityRow> entityRows, FeatureSetSpec featureSetSpec) {
    String featureSetRef = generateFeatureSetStringRef(featureSetSpec);
    List<String> featureSetEntityNames =
        featureSetSpec.getEntitiesList().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toList());
    List<BigtableKey> bigtableKeys =
        entityRows.stream()
            .map(row -> makeBigtableKey(featureSetRef, featureSetEntityNames, row))
            .collect(Collectors.toList());
    return bigtableKeys;
  }

  /**
   * Create {@link BigtableKey}
   *
   * @param featureSet featureSet reference of the feature. E.g. feature_set_1
   * @param featureSetEntityNames entity names that belong to the featureSet
   * @param entityRow entityRow to build the key from
   * @return {@link BigtableKey}
   */
  private BigtableKey makeBigtableKey(
      String featureSet, List<String> featureSetEntityNames, EntityRow entityRow) {
    BigtableKey.Builder builder = BigtableKey.newBuilder().setFeatureSet(featureSet);
    Map<String, Value> fieldsMap = entityRow.getFieldsMap();
    featureSetEntityNames.sort(String::compareTo);
    for (int i = 0; i < featureSetEntityNames.size(); i++) {
      String entityName = featureSetEntityNames.get(i);

      if (!fieldsMap.containsKey(entityName)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Entity row fields \"%s\" does not contain required entity field \"%s\"",
                    fieldsMap.keySet().toString(), entityName))
            .asRuntimeException();
      }

      builder.addEntities(
          Field.newBuilder().setName(entityName).setValue(fieldsMap.get(entityName)));
    }
    return builder.build();
  }

  private List<FeatureRow> sendAndProcessMultiGet(
      List<BigtableKey> bigtableKeys,
      FeatureSetSpec featureSetSpec,
      List<FeatureReference> featureReferences)
      throws InvalidProtocolBufferException, ExecutionException {


    List<ByteString> refs = featureReferences.stream().map(featureReference -> ByteString.copyFromUtf8(featureReference.getName())).collect(Collectors.toList());
    List<byte[]> values = sendMultiGet(bigtableKeys, refs.get(0), refs.get(refs.size()-1));
    List<FeatureRow> featureRows = new ArrayList<>();

    FeatureRow.Builder nullFeatureRowBuilder =
        FeatureRow.newBuilder().setFeatureSet(generateFeatureSetStringRef(featureSetSpec));
    for (FeatureReference featureReference : featureReferences) {
      nullFeatureRowBuilder.addFields(Field.newBuilder().setName(featureReference.getName()));
    }

    for (int i = 0; i < values.size(); i++) {

      byte[] value = values.get(i);
      if (value == null) {
        featureRows.add(nullFeatureRowBuilder.build());
        continue;
      }

      FeatureRow featureRow = FeatureRow.parseFrom(value);
      String featureSetRef = bigtableKeys.get(i).getFeatureSet();
      FeatureRowDecoder decoder = new FeatureRowDecoder(featureSetRef, featureSetSpec);
      if (decoder.isEncoded(featureRow)) {
        if (decoder.isEncodingValid(featureRow)) {
          featureRow = decoder.decode(featureRow);
        } else {
          featureRows.add(nullFeatureRowBuilder.build());
          continue;
        }
      }

      featureRows.add(featureRow);
    }
    return featureRows;
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of {@link BigtableKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link BigtableKey}
   */
  private List<byte[]> sendMultiGet(List<BigtableKey> keys, ByteString startSuffix, ByteString endSuffix) {
    ByteString[][] binaryKeys =
            keys.stream()
                    .map(AbstractMessageLite::toByteString)
                    .collect(Collectors.toList())
                    .toArray(new ByteString[0][0]);
    ByteStringRange keyRange = ByteStringRange.create(startSuffix, endSuffix).endClosed(endSuffix);
    try {
      return dataClient.readRows(Query.create(table).range(keyRange)).wait()
          .map(
              keyValue -> {
                if (keyValue == null) {
                  return null;
                }
                return keyValue.getValueOrElse(null);
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw Status.NOT_FOUND
          .withDescription("Unable to retrieve feature from Bigtable")
          .withCause(e)
          .asRuntimeException();
    }
  }

  // TODO: Refactor this out to common package?
  private static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    String ref = String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
    return ref;
  }
}
