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

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class BigtableOnlineRetriever implements OnlineRetriever {

  private final BigtableDataClient dataClient;

  private BigtableOnlineRetriever(BigtableDataClient dataClient) {
    this.dataClient = dataClient;
  }

  public static OnlineRetriever create(Map<String, String> config) {

    BigtableDataClient dataClient = BigtableDataClient.create(config.get("projectId"), config.get("instanceId"));
    return new BigtableOnlineRetriever(dataClient);
  }

  public static OnlineRetriever create(BigtableDataClient dataClient) {
    return new BigtableOnlineRetriever(dataClient);
  }

  /**
   * Gets online features from bigtable. This method returns a list of {@link FeatureRow}s
   * corresponding to each feature set spec. Each feature row in the list then corresponds to an
   * {@link EntityRow} provided by the user.
   *
   * @param entityRows list of entity rows in the feature request
   * @param featureSetRequests Map of {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} to
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

    List<byte[]> values = sendMultiGet(bigtableKeys);
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
  private List<byte[]> sendMultiGet(List<BigtableKey> keys, String table) {
    try {
      return dataClient.readRowsAsync(table, keys).stream()
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
