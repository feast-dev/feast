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
package dev.feast;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.serving.ServingServiceGrpc.ServingServiceFutureStub;
import feast.proto.types.ValueProto;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import io.opentracing.util.GlobalTracer;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class FeastClient implements AutoCloseable {
  Logger logger = LoggerFactory.getLogger(FeastClient.class);

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;
  private static final int DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024; // 64MB
  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final Duration DEFAULT_KEEP_ALIVE_TIME = Duration.ofSeconds(60);
  private static final Duration DEFAULT_KEEP_ALIVE_TIMEOUT = Duration.ofSeconds(20);

  private final ManagedChannel channel;
  private final ServingServiceBlockingStub blockingStub;
  private final ServingServiceFutureStub asyncStub;
  private final int batchSize;

  /**
   * Create a client to access Feast Serving.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @return {@link FeastClient}
   */
  public static FeastClient create(String host, int port) {
    // configure client with no security config.
    return FeastClient.createSecure(host, port, SecurityConfig.newBuilder().build());
  }

  /**
   * Create a authenticated client that can access Feast serving with authentication enabled.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @param securityConfig security options to configure the Feast client. See {@link
   *     SecurityConfig} for options.
   * @return {@link FeastClient}
   */
  public static FeastClient createSecure(String host, int port, SecurityConfig securityConfig) {
    ManagedChannel channel = null;
    if (securityConfig.isTLSEnabled()) {
      NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port)
          .maxInboundMessageSize(DEFAULT_MAX_MESSAGE_SIZE)
          .maxInboundMetadataSize(DEFAULT_MAX_MESSAGE_SIZE)
          .keepAliveTime(DEFAULT_KEEP_ALIVE_TIME.toMillis(), TimeUnit.MILLISECONDS)
          .keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .keepAliveWithoutCalls(true);

      if (securityConfig.getCertificatePath().isPresent()) {
        String certificatePath = securityConfig.getCertificatePath().get();
        try {
          builder.useTransportSecurity()
              .sslContext(GrpcSslContexts.forClient().trustManager(new File(certificatePath)).build());
        } catch (SSLException e) {
          throw new IllegalArgumentException(
              String.format("Invalid Certificate provided at path: %s", certificatePath), e);
        }
      } else {
        builder.useTransportSecurity();
      }
      channel = builder.build();
    } else {
      channel = ManagedChannelBuilder.forAddress(host, port)
          .maxInboundMessageSize(DEFAULT_MAX_MESSAGE_SIZE)
          .usePlaintext()
          .build();
    }

    return new FeastClient(channel, securityConfig.getCredentials());
  }

  /**
   * Obtain info about Feast Serving.
   *
   * @return {@link GetFeastServingInfoResponse} containing Feast version, Serving type etc.
   */
  public GetFeastServingInfoResponse getFeastServingInfo() {
    return blockingStub.getFeastServingInfo(GetFeastServingInfoRequest.newBuilder().build());
  }

  /**
   * Get online features from Feast, without indicating project, will use `default`.
   *
   * <p>See {@link #getOnlineFeatures(List, List, String)}
   *
   * @param featureRefs list of string feature references to retrieve in the following format
   *     featureTable:feature, where 'featureTable' and 'feature' refer to the FeatureTable and
   *     Feature names respectively. Only the Feature name is required.
   * @param entities list of {@link Row} to select the entities to retrieve the features for.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(List<String> featureRefs, List<Row> entities) {
    GetOnlineFeaturesRequest.Builder requestBuilder = GetOnlineFeaturesRequest.newBuilder();

    requestBuilder.setFeatures(
        ServingAPIProto.FeatureList.newBuilder().addAllVal(featureRefs).build());

    requestBuilder.putAllEntities(getEntityValuesMap(entities));

    GetOnlineFeaturesResponse response = blockingStub.getOnlineFeatures(requestBuilder.build());

    List<Row> results = Lists.newArrayList();
    if (response.getResultsCount() == 0) {
      return results;
    }

    for (int rowIdx = 0; rowIdx < response.getResults(0).getValuesCount(); rowIdx++) {
      Row row = Row.create();
      for (int featureIdx = 0; featureIdx < response.getResultsCount(); featureIdx++) {
        row.set(
            response.getMetadata().getFeatureNames().getVal(featureIdx),
            response.getResults(featureIdx).getValues(rowIdx),
            response.getResults(featureIdx).getStatuses(rowIdx));

        row.setEntityTimestamp(
            Instant.ofEpochSecond(
                response.getResults(featureIdx).getEventTimestamps(rowIdx).getSeconds()));
      }
      for (Map.Entry<String, ValueProto.Value> entry :
          entities.get(rowIdx).getFields().entrySet()) {
        row.set(entry.getKey(), entry.getValue());
      }

      results.add(row);
    }
    return results;
  }

  private Map<String, ValueProto.RepeatedValue> getEntityValuesMap(List<Row> entities) {
    Map<String, ValueProto.RepeatedValue.Builder> columnarEntities = new HashMap<>();
    for (Row row : entities) {
      for (Map.Entry<String, ValueProto.Value> field : row.getFields().entrySet()) {
        if (!columnarEntities.containsKey(field.getKey())) {
          columnarEntities.put(field.getKey(), ValueProto.RepeatedValue.newBuilder());
        }
        columnarEntities.get(field.getKey()).addVal(field.getValue());
      }
    }

    return columnarEntities.entrySet().stream()
        .map((e) -> Map.entry(e.getKey(), e.getValue().build()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Get online features from Feast.
   *
   * <p>Example of retrieving online features for the driver FeatureTable, with features driver_id
   * and driver_name
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
   *     featureTable:feature, where 'featureTable' and 'feature' refer to the FeatureTable and
   *     Feature names respectively. Only the Feature name is required.
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param project {@link String} Specifies the project override. If specified uses the project for
   *     retrieval. Overrides the projects set in Feature References if also specified.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(List<String> featureRefs, List<Row> rows, String project) {
    return getOnlineFeatures(featureRefs, rows);
  }

  public CompletableFuture<List<Row>> getOnlineFeaturesAsync(
      List<String> featureRefs, List<Row> entities) {
    return getOnlineFeaturesAsync(featureRefs, entities, null);
  }

  public CompletableFuture<List<Row>> getOnlineFeaturesAsync(
      List<String> featureRefs, List<Row> entities, String project) {
    
    GetOnlineFeaturesRequest.Builder requestBuilder = GetOnlineFeaturesRequest.newBuilder();
    requestBuilder.setFeatures(
        ServingAPIProto.FeatureList.newBuilder().addAllVal(featureRefs).build());
    requestBuilder.putAllEntities(getEntityValuesMap(entities));

    GetOnlineFeaturesRequest request = requestBuilder.build();
    ListenableFuture<GetOnlineFeaturesResponse> future = asyncStub.getOnlineFeatures(request);
    
    CompletableFuture<List<Row>> result = new CompletableFuture<>();
    future.addListener(() -> {
      try {
        GetOnlineFeaturesResponse response = future.get();
        result.complete(processOnlineFeaturesResponse(response, entities));
      } catch (Exception e) {
        result.completeExceptionally(e);
      }
    }, MoreExecutors.directExecutor());
    
    return result;
  }

  private List<Row> processOnlineFeaturesResponse(
      GetOnlineFeaturesResponse response, List<Row> entities) {
    List<Row> results = Lists.newArrayList();
    if (response.getResultsCount() == 0) {
      return results;
    }

    for (int rowIdx = 0; rowIdx < response.getResults(0).getValuesCount(); rowIdx++) {
      Row row = Row.create();
      for (int featureIdx = 0; featureIdx < response.getResultsCount(); featureIdx++) {
        row.set(
            response.getMetadata().getFeatureNames().getVal(featureIdx),
            response.getResults(featureIdx).getValues(rowIdx),
            response.getResults(featureIdx).getStatuses(rowIdx));

        row.setEntityTimestamp(
            Instant.ofEpochSecond(
                response.getResults(featureIdx).getEventTimestamps(rowIdx).getSeconds()));
      }
      
      // Add entity fields
      entities.get(rowIdx).getFields().forEach(row::set);
      results.add(row);
    }
    return results;
  }

  public List<Row> getOnlineFeaturesInBatches(
      List<String> featureRefs, List<Row> entities, String project) {
    List<Row> results = new ArrayList<>();
    
    for (int i = 0; i < entities.size(); i += batchSize) {
      int end = Math.min(entities.size(), i + batchSize);
      List<Row> batch = entities.subList(i, end);
      results.addAll(getOnlineFeatures(featureRefs, batch, project));
    }
    
    return results;
  }

  protected FeastClient(ManagedChannel channel, Optional<CallCredentials> credentials) {
    this.channel = channel;
    this.batchSize = DEFAULT_BATCH_SIZE;
    TracingClientInterceptor tracingInterceptor =
        TracingClientInterceptor.newBuilder().withTracer(GlobalTracer.get()).build();

    ServingServiceBlockingStub servingStub =
        ServingServiceGrpc.newBlockingStub(tracingInterceptor.intercept(channel));

    ServingServiceFutureStub asyncServingStub =
        ServingServiceGrpc.newFutureStub(tracingInterceptor.intercept(channel));

    if (credentials.isPresent()) {
      servingStub = servingStub.withCallCredentials(credentials.get());
      asyncServingStub = asyncServingStub.withCallCredentials(credentials.get());
    }

    this.blockingStub = servingStub;
    this.asyncStub = asyncServingStub;
  }

  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }
  }
}
