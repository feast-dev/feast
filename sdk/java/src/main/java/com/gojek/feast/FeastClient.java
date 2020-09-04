/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package com.gojek.feast;

import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class FeastClient implements AutoCloseable {
  Logger logger = LoggerFactory.getLogger(FeastClient.class);

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;

  private final ManagedChannel channel;
  private final ServingServiceBlockingStub stub;

  /**
   * Create a client to access Feast Serving.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @return {@link FeastClient}
   */
  public static FeastClient create(String host, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    return new FeastClient(channel, Optional.empty());
  }

  /**
   * Create a authenticated client that can access Feast serving with authentication enabled.
   * Supports the {@link CallCredentials} in the {@link feast.common.auth.credentials} package.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @param credentials Call credentials used to provide credentials when calling Feast.
   * @return {@link FeastClient}
   */
  public static FeastClient createAuthenticated(
      String host, int port, CallCredentials credentials) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    return new FeastClient(channel, Optional.of(credentials));
  }

  /**
   * Obtain info about Feast Serving.
   *
   * @return {@link feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse} containing
   *     Feast version, Serving type etc.
   */
  public GetFeastServingInfoResponse getFeastServingInfo() {
    return stub.getFeastServingInfo(GetFeastServingInfoRequest.newBuilder().build());
  }

  /**
   * Get online features from Feast from FeatureSets
   *
   * <p>See {@link #getOnlineFeatures(List, List, String, boolean)}
   *
   * @param featureRefs list of string feature references to retrieve in the following format
   *     featureSet:feature, where 'featureSet' and 'feature' refer to the FeatureSet and Feature
   *     names respectively. Only the Feature name is required.
   * @param rows list of {@link Row} to select the entities to retrieve the features for.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(List<String> featureRefs, List<Row> rows) {
    return getOnlineFeatures(featureRefs, rows, "");
  }

  /**
   * Get online features from Feast.
   *
   * <p>See {@link #getOnlineFeatures(List, List, String, boolean)}
   *
   * @param featureRefs list of string feature references to retrieve in the following format
   *     featureSet:feature, where 'featureSet' and 'feature' refer to the FeatureSet and Feature
   *     names respectively. Only the Feature name is required.
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param project {@link String} Specifies the project override. If specifed uses the project for
   *     retrieval. Overrides the projects set in Feature References if also specified.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(List<String> featureRefs, List<Row> rows, String project) {
    return getOnlineFeatures(featureRefs, rows, project, false);
  }

  /**
   * Get online features from Feast.
   *
   * <p>Example of retrieving online features for the driver featureset, with features driver_id and
   * driver_name
   *
   * <pre>{@code
   * FeastClient client = FeastClient.create("localhost", 6566);
   * List<String> requestedFeatureIds = Arrays.asList("driver:driver_id", "driver:driver_name");
   * List<Row> requestedRows =
   *         Arrays.asList(Row.create().set("driver_id", 123), Row.create().set("driver_id", 456));
   * List<Row> retrievedFeatures = client.getOnlineFeatures(requestedFeatureIds, requestedRows);
   * retrievedFeatures.forEach(System.out::println);
   * }</pre>
   *
   * @param featureRefs list of string feature references to retrieve in the following format
   *     featureSet:feature, where 'featureSet' and 'feature' refer to the FeatureSet and Feature
   *     names respectively. Only the Feature name is required.
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param project {@link String} Specifies the project override. If specifed uses the project for
   *     retrieval. Overrides the projects set in Feature References if also specified.
   * @param omitEntitiesInResponse if true, the returned {@link Row} will not contain field and
   *     value for the entity
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(
      List<String> featureRefs, List<Row> rows, String project, boolean omitEntitiesInResponse) {
    List<FeatureReference> features = RequestUtil.createFeatureRefs(featureRefs);
    // build entity rows and collect entity references
    HashSet<String> entityRefs = new HashSet<>();
    List<EntityRow> entityRows =
        rows.stream()
            .map(
                row -> {
                  entityRefs.addAll(row.getFields().keySet());
                  return EntityRow.newBuilder()
                      .setEntityTimestamp(row.getEntityTimestamp())
                      .putAllFields(row.getFields())
                      .build();
                })
            .collect(Collectors.toList());

    GetOnlineFeaturesResponse response =
        stub.getOnlineFeatures(
            GetOnlineFeaturesRequest.newBuilder()
                .addAllFeatures(features)
                .addAllEntityRows(entityRows)
                .setProject(project)
                .setOmitEntitiesInResponse(omitEntitiesInResponse)
                .build());

    return response.getFieldValuesList().stream()
        .map(
            fieldValues -> {
              Row row = Row.create();
              for (String fieldName : fieldValues.getFieldsMap().keySet()) {
                row.set(
                    fieldName,
                    fieldValues.getFieldsMap().get(fieldName),
                    fieldValues.getStatusesMap().get(fieldName));
              }
              return row;
            })
        .collect(Collectors.toList());
  }

  protected FeastClient(ManagedChannel channel, Optional<CallCredentials> credentials) {
    this.channel = channel;
    ServingServiceBlockingStub servingStub = ServingServiceGrpc.newBlockingStub(channel);
    if (credentials.isPresent()) {
      servingStub = servingStub.withCallCredentials(credentials.get());
    }
    this.stub = servingStub;
  }

  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }
  }
}
