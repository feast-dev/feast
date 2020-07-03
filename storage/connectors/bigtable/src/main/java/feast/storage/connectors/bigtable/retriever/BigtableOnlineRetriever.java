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

import static com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.bigtable.repackaged.com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Query;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
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
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class BigtableOnlineRetriever implements OnlineRetriever {

  private final BigtableDataClient dataClient;
  private final String table;

  private BigtableOnlineRetriever(BigtableDataClient dataClient, String table) {
    this.dataClient = dataClient;
    this.table = table;
  }

  public static OnlineRetriever create(Map<String, String> config) {
    BigtableDataClient dataClient;
    try {
      dataClient = BigtableDataClient.create(config.get("project_id"), config.get("instance_id"));
    } catch (NullPointerException e) {
      throw new IllegalStateException(
          String.format("Unable to connect to bigtable with config %s", config.toString()), e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalStateException(
          String.format("Unable to connect to bigtable with config %s", config.toString()), e);
    }
    return new BigtableOnlineRetriever(dataClient, config.get("table_id"));
  }

  public static OnlineRetriever create(BigtableDataClient dataClient, String tableId) {
    return new BigtableOnlineRetriever(dataClient, tableId);
  }

  /**
   * Gets online features from bigtable. This method returns a list of {@link FeatureRow}s
   * corresponding to each feature set spec. Each feature row in the list then corresponds to an
   * {@link EntityRow} provided by the user.
   *
   * @param entityRows list of entity rows in the feature request
   * @param featureSetRequest Map of {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} to
   *     feature references in the request tied to that feature set.
   * @return List of List of {@link FeatureRow}
   */
  @Override
  public List<Optional<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, FeatureSetRequest featureSetRequest) {
    FeatureSetSpec featureSetSpec = featureSetRequest.getSpec();
    List<String> bigtableKeys = buildBigtableKeys(entityRows, featureSetSpec);
    List<Optional<FeatureRow>> featureRows;
    ImmutableList<FeatureReference> featureRequestList =
        featureSetRequest.getFeatureReferences().asList();
    FeatureRowDecoder decoder =
        new FeatureRowDecoder(
            generateFeatureSetStringRef(featureSetSpec), featureSetSpec, featureRequestList);
    try {
      featureRows = getFeaturesFromBigtable(bigtableKeys, featureRequestList, decoder);
    } catch (InvalidProtocolBufferException | ExecutionException e) {
      throw Status.INTERNAL
          .withDescription("Unable to parse protobuf while retrieving feature")
          .withCause(e)
          .asRuntimeException();
    }
    return featureRows;
  }

  private List<Optional<FeatureRow>> getFeaturesFromBigtable(
      List<String> bigtableKeys,
      ImmutableList<FeatureReference> featureReferences,
      FeatureRowDecoder decoder)
      throws InvalidProtocolBufferException, ExecutionException {
    // pull feature row data bytes from redis using given redis keys
    ServerStream<Row> featureRowsStream = sendMultiGet(bigtableKeys, featureReferences);
    List<Optional<FeatureRow>> featureRows = new ArrayList<>();
    for (Row featureRow : featureRowsStream) {
      try {
        FeatureRow decodedFeatureRow = decoder.decode(featureRow);
        featureRows.add(Optional.of(decodedFeatureRow));
      } catch (Exception e) {
        // decoding feature row failed: data corruption could have occurred
        e.printStackTrace();
        throw Status.DATA_LOSS
            .withCause(e.getCause())
            .withDescription(
                "Failed to decode FeatureRow from Row received from bigtable"
                    + ": Possible data corruption")
            .asRuntimeException();
      }
    }
    return featureRows;
  }

  private List<String> buildBigtableKeys(
      List<EntityRow> entityRows, FeatureSetSpec featureSetSpec) {
    String featureSetRef = generateFeatureSetStringRef(featureSetSpec);
    List<String> featureSetEntityNames =
        featureSetSpec.getEntitiesList().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toList());
    List<BigtableKey> bigtableKeys =
        entityRows.stream()
            .map(row -> makeBigtableKey(featureSetRef, featureSetEntityNames, row))
            .collect(Collectors.toList());
    List<String> btKeys = new ArrayList<>();
    for (BigtableKey btk : bigtableKeys) {
      StringBuilder bigtableKey = new StringBuilder();
      for (Field field : btk.getEntitiesList()) {
        bigtableKey.append(field.getValue().getStringVal());
      }
      bigtableKey.append("#");
      for (Field field : btk.getEntitiesList()) {
        bigtableKey.append(field.getName());
        bigtableKey.append("#");
      }
      bigtableKey.append(featureSetSpec.getProject());
      bigtableKey.append("#");
      bigtableKey.append(featureSetSpec.getName());
      btKeys.add(bigtableKey.toString());
    }
    return btKeys;
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
    for (String entityName : featureSetEntityNames) {
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

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of {@link BigtableKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link
   *     BigtableKey}
   */
  private ServerStream<Row> sendMultiGet(
      List<String> keys, ImmutableList<FeatureReference> featureReferences) {
    try {
      Query multiGet = Query.create(table);
      StringBuilder regexString = new StringBuilder();
      regexString.append("(?i)(\\W|^)(");
      for (FeatureReference reference : featureReferences) {
        regexString.append(reference.getName());
        regexString.append("|");
      }
      regexString.append("event_timestamp");
      regexString.append(")(\\W|$)");
      Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
      Filters.Filter colFilter = FILTERS.qualifier().regex(regexString.toString());
      for (String key : keys) {
        multiGet.rowKey(key).filter(filter);
      }
      return dataClient.readRows(multiGet);
    } catch (Exception e) {
      throw Status.NOT_FOUND
          .withDescription("Unable to retrieve feature from Bigtable")
          .withCause(e)
          .asRuntimeException();
    }
  }

  // TODO: Refactor this out to common package?
  private static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
  }
}
