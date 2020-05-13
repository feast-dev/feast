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

import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class FeastClient implements AutoCloseable {
  Logger logger = LoggerFactory.getLogger(FeastClient.class);

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;

  private final ManagedChannel channel;
  private final ServingServiceGrpc.ServingServiceBlockingStub stub;

  /**
   * Create a client to access Feast
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @return {@link FeastClient}
   */
  public static FeastClient create(String host, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    return new FeastClient(channel);
  }

  public GetFeastServingInfoResponse getFeastServingInfo() {
    return stub.getFeastServingInfo(GetFeastServingInfoRequest.newBuilder().build());
  }

  /**
   * Get online features from Feast.
   *
   * <p>See {@link #getOnlineFeatures(List, List, String)}
   *
   * @param features list of string feature references to retrieve, feature reference follows this
   *     format [project]/[name]
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param defaultProject {@link String} Default project to find features in if not provided in
   *     feature reference.
   * @return list of {@link Row} containing features
   */
  public List<Row> getOnlineFeatures(List<String> features, List<Row> rows, String defaultProject) {
    return getOnlineFeatures(features, rows, defaultProject, false);
  }

  /**
   * Get online features from Feast.
   *
   * <p>Example of retrieving online features for the driver project, with features driver_id and
   * driver_name
   *
   * <pre>{@code
   * FeastClient client = FeastClient.create("localhost", 6566);
   * List<String> requestedFeatureIds = Arrays.asList("driver/driver_id", "driver/driver_name");
   * List<Row> requestedRows =
   *         Arrays.asList(Row.create().set("driver_id", 123), Row.create().set("driver_id", 456));
   * List<Row> retrievedFeatures = client.getOnlineFeatures(requestedFeatureIds, requestedRows);
   * retrievedFeatures.forEach(System.out::println);
   * }</pre>
   *
   * @param featureRefStrings list of feature refs to retrieve, feature refs follow this format
   *     [project]/[name]
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param defaultProject {@link String} Default project to find features in if not provided in
   *     feature reference.
   * @param omitEntitiesInResponse if true, the returned {@link Row} will not contain field and
   *     value for the entity
   * @return list of {@link Row} containing features
   */
  public List<Row> getOnlineFeatures(
      List<String> featureRefStrings,
      List<Row> rows,
      String defaultProject,
      boolean omitEntitiesInResponse) {
    List<FeatureReference> features =
        RequestUtil.createFeatureRefs(featureRefStrings, defaultProject);
    List<EntityRow> entityRows =
        rows.stream()
            .map(
                row ->
                    EntityRow.newBuilder()
                        .setEntityTimestamp(row.getEntityTimestamp())
                        .putAllFields(row.getFields())
                        .build())
            .collect(Collectors.toList());

    GetOnlineFeaturesResponse response =
        stub.getOnlineFeatures(
            GetOnlineFeaturesRequest.newBuilder()
                .addAllFeatures(features)
                .addAllEntityRows(entityRows)
                .setOmitEntitiesInResponse(omitEntitiesInResponse)
                .build());

    return response.getFieldValuesList().stream()
        .map(
            field -> {
              Row row = Row.create();
              field.getFieldsMap().forEach(row::set);
              return row;
            })
        .collect(Collectors.toList());
  }

  private FeastClient(ManagedChannel channel) {
    this.channel = channel;
    stub = ServingServiceGrpc.newBlockingStub(channel);
  }

  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }
  }
}
