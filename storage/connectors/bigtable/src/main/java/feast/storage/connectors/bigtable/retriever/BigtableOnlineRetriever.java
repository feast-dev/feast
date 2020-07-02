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

import com.google.bigtable.repackaged.com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Query;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.FeatureSetProto;
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
  private static final String METADATA_CF = "metadata";
  private static final String FEATURES_CF = "features";

  private BigtableOnlineRetriever(BigtableDataClient dataClient, String table) {
    this.dataClient = dataClient;
    this.table = table;
  }

  public static OnlineRetriever create(Map<String, String> config) {
    BigtableDataClient dataClient = null;
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
    List<Optional<FeatureRow>> featureRows = new ArrayList<>();
    HashMap<FeatureSetRequest, List<String>> keyMap = new HashMap<>();
    System.out.printf("Computing Bigtable Keys\n");
    List<String> bigtableKeys = buildBigtableKeys(entityRows, featureSetRequest.getSpec());
    keyMap.put(featureSetRequest, bigtableKeys);
    try {
      System.out.printf("Bigtable Keys %s\f", bigtableKeys.toString());
      featureRows =
          sendAndProcessMultiGet(
              bigtableKeys,
              featureSetRequest.getSpec(),
              featureSetRequest.getFeatureReferences().asList());
    } catch (InvalidProtocolBufferException | ExecutionException e) {
      throw Status.INTERNAL
          .withDescription("Unable to parse protobuf while retrieving feature")
          .withCause(e)
          .asRuntimeException();
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

  private List<Optional<FeatureRow>> sendAndProcessMultiGet(
      List<String> bigtableKeys,
      FeatureSetSpec featureSetSpec,
      List<FeatureReference> featureReferences)
      throws InvalidProtocolBufferException, ExecutionException {
    List<ByteString> refs =
        featureReferences.stream()
            .map(featureReference -> ByteString.copyFromUtf8(featureReference.getName()))
            .collect(Collectors.toList());
    return sendMultiGet(bigtableKeys, featureSetSpec, featureReferences);
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of {@link BigtableKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link
   *     BigtableKey}
   */
  private List<Optional<FeatureRow>> sendMultiGet(
      List<String> keys, FeatureSetSpec featureSetSpec, List<FeatureReference> featureReferences) {
    HashMap<String, FeatureRow.Builder> resultMap = new HashMap<>();
    HashMap<ByteString, Descriptors.Descriptor> featureMap = new HashMap<>();
    List<FeatureSetProto.FeatureSpec> featuresList = featureSetSpec.getFeaturesList();
    HashMap<String, Descriptors.Descriptor> allFeatureMap = new HashMap<>();
    for (FeatureSetProto.FeatureSpec spec : featuresList) {
      allFeatureMap.put(spec.getName(), spec.getDescriptorForType());
    }
    FeatureRow.Builder nullFeatureRowBuilder =
        FeatureRow.newBuilder().setFeatureSet(generateFeatureSetStringRef(featureSetSpec));

    for (FeatureReference featureReference : featureReferences) {
      nullFeatureRowBuilder.addFields(Field.newBuilder().setName(featureReference.getName()));
      featureMap.put(
          ByteString.copyFromUtf8(featureReference.getName()),
          allFeatureMap.get(featureReference.getName()));
    }
    for (String bigtableKey : keys) {
      System.out.printf("Pushing the bigtable key %s\n", bigtableKey);
      resultMap.put(bigtableKey, nullFeatureRowBuilder);
    }
    try {
      Query multiGet = Query.create(table);
      for (String key : keys) {
        multiGet.rowKey(key);
      }
      ServerStream<Row> rows = dataClient.readRows(multiGet);
      for (Row row : rows) {
        List<RowCell> metadata = row.getCells(METADATA_CF);
        System.out.printf("Got a result row with key %s\n", row.getKey().toString());
        System.out.printf("OG key %s\n", row.getKey());
        FeatureRow.Builder resultBuilder = resultMap.get(row.getKey().toString());
        for (RowCell rowValues : row.getCells(FEATURES_CF)) {
          try {
            if (featureMap.containsKey(rowValues.getQualifier())) {
              System.out.printf(
                  "Adding the feature %s to %s\n", rowValues.getQualifier(), row.getKey());
              resultBuilder.addFields(
                  Field.newBuilder()
                      .setNameBytes(ByteString.copyFrom(rowValues.getQualifier().toByteArray()))
                      .setValue(
                          Value.newBuilder()
                              .setField(
                                  featureMap
                                      .get(rowValues.getQualifier())
                                      .findFieldByName(rowValues.getQualifier().toStringUtf8()),
                                  rowValues.getValue())
                              .build())
                      .build());
            }
          } catch (Exception e) {
            System.out.printf(
                "Failed to compute correctly for qualifier %s\n", rowValues.getQualifier());
            System.out.printf("Feature map keys %s\n", featureMap.keySet());
            throw Status.NOT_FOUND
                .withCause(e)
                .withDescription("There was a problem getting the right cells")
                .asRuntimeException();
          }
        }
      }
      List<Optional<FeatureRow>> result = new ArrayList<>();
      for (String key : resultMap.keySet()) {
        FeatureRow finalRow = resultMap.get(key).build();
        result.add(Optional.of(finalRow));
      }
      return result;
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
