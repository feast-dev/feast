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
import dev.feast.exception.FeastException;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRangeRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRangeResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.ValueProto;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import io.opentracing.util.GlobalTracer;
import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class FeastClient implements AutoCloseable {
  Logger logger = LoggerFactory.getLogger(FeastClient.class);

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;

  private final ManagedChannel channel;
  private final ServingServiceBlockingStub stub;
  private final long requestTimeout;

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
   * Create a client to access Feast Serving.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @param requestTimeout maximum duration for online retrievals from the GRPC server in
   *     milliseconds, use 0 for no timeout
   * @return {@link FeastClient}
   */
  public static FeastClient create(String host, int port, long requestTimeout) {
    // configure client with no security config.
    return FeastClient.createSecure(
        host, port, SecurityConfig.newBuilder().build(), requestTimeout, Optional.empty());
  }

  /**
   * Create an authenticated client that can access Feast serving with authentication enabled.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @param securityConfig security options to configure the Feast client. See {@link
   *     SecurityConfig} for options.
   * @return {@link FeastClient}
   */
  public static FeastClient createSecure(String host, int port, SecurityConfig securityConfig) {
    return FeastClient.createSecure(host, port, securityConfig, 0, Optional.empty());
  }

  /**
   * Create an authenticated client that can access Feast serving with authentication enabled and
   * has an optional serviceConfig.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @param requestTimeout maximum duration for online retrievals from the GRPC server in
   *     milliseconds
   * @param securityConfig security options to configure the Feast client. See {@link
   *     SecurityConfig} for options.
   * @param serviceConfig NettyChannel uses this serviceConfig to declare HTTP/2.0 protocol config
   *     options.
   * @return {@link FeastClient}
   */
  public static FeastClient createSecure(
      String host,
      int port,
      long requestTimeout,
      SecurityConfig securityConfig,
      Optional<Map<String, Object>> serviceConfig) {
    return FeastClient.createSecure(host, port, securityConfig, requestTimeout, serviceConfig);
  }

  /**
   * Create an authenticated client that can access Feast serving with authentication enabled.
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @param securityConfig security options to configure the Feast client. See {@link
   *     SecurityConfig} for options.
   * @param requestTimeout maximum duration for online retrievals from the GRPC server in
   *     milliseconds
   * @param serviceConfig NettyChannel uses this serviceConfig to declare HTTP/2.0 protocol config
   *     options.
   * @return {@link FeastClient}
   */
  public static FeastClient createSecure(
      String host,
      int port,
      SecurityConfig securityConfig,
      long requestTimeout,
      Optional<Map<String, Object>> serviceConfig) {

    if (requestTimeout < 0) {
      throw new IllegalArgumentException("Request timeout can't be negative");
    }

    // Configure client TLS
    NettyChannelBuilder nettyChannelBuilder = NettyChannelBuilder.forAddress(host, port);

    if (serviceConfig.isPresent()) {
      nettyChannelBuilder.defaultServiceConfig(serviceConfig.get());
    }

    if (securityConfig.isTLSEnabled()) {
      if (securityConfig.getCertificatePath().isPresent()) {
        String certificatePath = securityConfig.getCertificatePath().get();
        // Use custom certificate for TLS
        File certificateFile = new File(certificatePath);
        try {
          nettyChannelBuilder
              .useTransportSecurity()
              .sslContext(GrpcSslContexts.forClient().trustManager(certificateFile).build());
        } catch (SSLException e) {
          throw new IllegalArgumentException(
              String.format("Invalid Certificate provided at path: %s", certificatePath), e);
        }
      } else {
        // Use system certificates for TLS
        nettyChannelBuilder.useTransportSecurity();
      }
    } else {
      // Disable TLS
      nettyChannelBuilder.usePlaintext();
    }

    return new FeastClient(
        nettyChannelBuilder.build(), securityConfig.getCredentials(), requestTimeout);
  }

  /**
   * Obtain info about Feast Serving.
   *
   * @return {@link GetFeastServingInfoResponse} containing Feast version, Serving type etc.
   */
  public GetFeastServingInfoResponse getFeastServingInfo() {
    return stub.getFeastServingInfo(GetFeastServingInfoRequest.newBuilder().build());
  }

  /**
   * Get online features from Feast, without indicating project, will use 'default'
   *
   * @param featureService string representing the name of the featureService to call. Internally
   *     this results in a call to the registry which resolves the featureNames.
   * @param entities list of {@link Row} to select the entities to retrieve the features for.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(String featureService, List<Row> entities) {
    GetOnlineFeaturesRequest.Builder requestBuilder = GetOnlineFeaturesRequest.newBuilder();

    requestBuilder.setFeatureService(featureService);

    requestBuilder.putAllEntities(transposeEntitiesOntoColumns(entities));

    List<Row> resp = fetchOnlineFeatures(requestBuilder.build(), entities);

    if (resp.size() == 0) {
      logger.info(
          "Result was empty for getOnlineFeatures call with feature service name: {}, entities: {}",
          featureService,
          entities);
    }

    return resp;
  }

  /**
   * Get online features from Feast with feature service, specifying includeMetadata
   *
   * @param featureService string representing the name of the featureService to call. Internally
   *     this results in a call to the registry which resolves the featureNames.
   * @param entities list of {@link Row} to select the entities to retrieve the features for.
   * @param includeMetadata boolean indicating whether to include metadata in the response.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(
      String featureService, List<Row> entities, boolean includeMetadata) {
    GetOnlineFeaturesRequest.Builder requestBuilder = GetOnlineFeaturesRequest.newBuilder();

    requestBuilder.setFeatureService(featureService);
    requestBuilder.putAllEntities(transposeEntitiesOntoColumns(entities));
    requestBuilder.setIncludeMetadata(includeMetadata);

    List<Row> resp = fetchOnlineFeatures(requestBuilder.build(), entities);

    if (resp.size() == 0) {
      logger.info(
          "Result was empty for getOnlineFeatures call with feature service name: {}, entities: {}",
          featureService,
          entities);
    }

    return resp;
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
   * @param includeMetadata boolean indicating whether to include metadata in the response.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(
      List<String> featureRefs, List<Row> entities, boolean includeMetadata) {
    GetOnlineFeaturesRequest.Builder requestBuilder = GetOnlineFeaturesRequest.newBuilder();

    requestBuilder.setFeatures(
        ServingAPIProto.FeatureList.newBuilder().addAllVal(featureRefs).build());

    requestBuilder.putAllEntities(transposeEntitiesOntoColumns(entities));

    requestBuilder.setIncludeMetadata(includeMetadata);

    List<Row> resp = fetchOnlineFeatures(requestBuilder.build(), entities);

    if (resp.size() == 0) {
      logger.info(
          "Result was empty for getOnlineFeatures call with featureRefs: {}, entities: {}",
          featureRefs,
          entities);
    }

    return resp;
  }

  public List<Row> getOnlineFeatures(
      GetOnlineFeaturesRequest getOnlineFeaturesRequest, List<Row> entities) {
    if (getOnlineFeaturesRequest.getFeatureService().isEmpty()
        && getOnlineFeaturesRequest.getFeatures().getValCount() == 0) {
      logger.info(
          "Neither a featureService or featureRef was present in the request with request: {}, entities: {}",
          getOnlineFeaturesRequest.toString(),
          entities);

      return Collections.emptyList();
    }

    List<Row> resp = fetchOnlineFeatures(getOnlineFeaturesRequest, entities);

    if (resp.size() == 0) {
      logger.info(
          "Result was empty for getOnlineFeatures call with getOnlineFeaturesRequest: {}, entities: {}",
          getOnlineFeaturesRequest.toString(),
          entities);
    }

    return resp;
  }

  // Internal method that fetches online features from feature server.
  private List<Row> fetchOnlineFeatures(
      GetOnlineFeaturesRequest getOnlineFeaturesRequest, List<Row> entities) {
    ServingServiceGrpc.ServingServiceBlockingStub timedStub =
        requestTimeout != 0 ? stub.withDeadlineAfter(requestTimeout, TimeUnit.MILLISECONDS) : stub;

    GetOnlineFeaturesResponse response;
    try {
      response = timedStub.getOnlineFeatures(getOnlineFeaturesRequest);
    } catch (StatusRuntimeException e) {
      throw FeastException.fromStatusException(e);
    }

    List<Row> results = Lists.newArrayList();
    if (response.getResultsCount() == 0) {
      return results;
    }

    for (int rowIdx = 0; rowIdx < response.getResults(0).getValuesCount(); rowIdx++) {
      Row row = Row.create();
      for (int featureIdx = 0; featureIdx < response.getResultsCount(); featureIdx++) {
        if (!getOnlineFeaturesRequest.getIncludeMetadata()) {
          row.set(
              response.getMetadata().getFeatureNames().getVal(featureIdx),
              response.getResults(featureIdx).getValues(rowIdx));
        } else {
          row.setWithFieldStatus(
              response.getMetadata().getFeatureNames().getVal(featureIdx),
              response.getResults(featureIdx).getValues(rowIdx),
              response.getResults(featureIdx).getStatuses(rowIdx));

          row.setEntityTimestamp(
              Instant.ofEpochSecond(
                  response.getResults(featureIdx).getEventTimestamps(rowIdx).getSeconds()));
        }
      }

      for (Map.Entry<String, ValueProto.Value> entry :
          entities.get(rowIdx).getFields().entrySet()) {
        if (!getOnlineFeaturesRequest.getIncludeMetadata()) {
          row.set(entry.getKey(), entry.getValue());
        } else {
          row.setWithFieldStatus(entry.getKey(), entry.getValue(), FieldStatus.PRESENT);
        }
      }

      results.add(row);
    }
    return results;
  }

  public Map<String, ValueProto.RepeatedValue> transposeEntitiesOntoColumns(List<Row> entities) {
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
   * Get online features from Feast via feature reference(s).
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
    return getOnlineFeatures(featureRefs, rows, false);
  }

  public List<Row> getOnlineFeatures(List<String> featureRefs, List<Row> rows) {
    return getOnlineFeatures(featureRefs, rows, false);
  }
  /**
   * Get online features from Feast given a feature service name. Internally feature service calls
   * resolve featureViews via a call to the feature registry.
   *
   * <p>Example of retrieving online features for the driver FeatureTable, with features driver_id
   * and driver_name
   *
   * <pre>{@code
   * FeastClient client = FeastClient.create("localhost", 6566);
   * List<String> requestedFeatureServiceName = "driver_service"
   * List<Row> requestedRows =
   *         Arrays.asList(Row.create().set("driver_id", 123), Row.create().set("driver_id", 456));
   * List<Row> retrievedFeatures = client.getOnlineFeatures(requestedFeatureServiceName, requestedRows);
   * retrievedFeatures.forEach(System.out::println);
   * }</pre>
   *
   * @param featureService name of the featureService object
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param project {@link String} Specifies the project override. If specified uses the project for
   *     retrieval. Overrides the projects set in Feature References if also specified.
   * @return list of {@link Row} containing retrieved data fields.
   */
  public List<Row> getOnlineFeatures(String featureService, List<Row> rows, String project) {
    return getOnlineFeatures(featureService, rows, false);
  }

  /**
   * Retrieves online features from Feast using the provided request, entity rows, and project name.
   *
   * @param getOnlineFeaturesRequest The request object containing feature references.
   * @param entities List of {@link Row} objects representing the entities to retrieve features for.
   * @param project The Feast project to retrieve features from.
   * @return A list of {@link Row} containing the retrieved feature data.
   */
  public List<Row> getOnlineFeatures(
      GetOnlineFeaturesRequest getOnlineFeaturesRequest, List<Row> entities, String project) {
    return getOnlineFeatures(getOnlineFeaturesRequest, entities);
  }

  /**
   * Get online features range from Feast without indicating a project — uses the default project.
   *
   * <p>See {@link #getOnlineFeaturesRange(List, List, List, int, boolean, String)} for
   * project-specific queries.
   *
   * @param request {@link GetOnlineFeaturesRangeRequest} containing the request parameters.
   * @param entities list of {@link Row} to select the entities to retrieve the features for.
   * @return list of {@link RangeRow} containing retrieved data fields.
   */
  public List<RangeRow> getOnlineFeaturesRange(
      GetOnlineFeaturesRangeRequest request, List<Row> entities) {

    ServingServiceGrpc.ServingServiceBlockingStub timedStub =
        requestTimeout != 0 ? stub.withDeadlineAfter(requestTimeout, TimeUnit.MILLISECONDS) : stub;

    GetOnlineFeaturesRangeResponse response;
    try {
      response = timedStub.getOnlineFeaturesRange(request);
    } catch (StatusRuntimeException e) {
      throw FeastException.fromStatusException(e);
    }

    List<RangeRow> results = Lists.newArrayList();

    List<String> featureRefs =
        request.hasFeatures() ? request.getFeatures().getValList() : Collections.emptyList();

    boolean includeMetadata = request.getIncludeMetadata();

    if (response.getResultsCount() == 0) {
      logger.info(
          "No results returned from Feast for getOnlineFeaturesRange with entities: {} and features: {}",
          entities,
          featureRefs);
      return results;
    }

    for (Map.Entry<String, ValueProto.RepeatedValue> entityEntry :
        response.getEntitiesMap().entrySet()) {
      if (entityEntry.getValue().getValCount() != response.getResults(0).getValuesCount()) {
        throw new IllegalStateException(
            String.format(
                "Entity %s has different number of values (%d) than feature rows (%d)",
                entityEntry.getKey(),
                entityEntry.getValue().getValCount(),
                response.getResults(0).getValuesCount()));
      }
    }

    for (int rowIdx = 0; rowIdx < response.getResults(0).getValuesCount(); rowIdx++) {
      RangeRow row = RangeRow.create();
      for (int featureIdx = 0; featureIdx < response.getResultsCount(); featureIdx++) {
        if (!includeMetadata) {
          row.setWithValues(
              response.getMetadata().getFeatureNames().getVal(featureIdx),
              response.getResults(featureIdx).getValues(rowIdx).getValList());
        } else {
          row.setWithValuesWithFieldStatus(
              response.getMetadata().getFeatureNames().getVal(featureIdx),
              response.getResults(featureIdx).getValues(rowIdx).getValList(),
              response.getResults(featureIdx).getStatuses(rowIdx).getStatusList());

          row.setEntityTimestamp(
              response.getResults(featureIdx).getEventTimestamps(rowIdx).getValList().stream()
                  .map(v -> Instant.ofEpochSecond(v.getUnixTimestampVal()))
                  .collect(Collectors.toList()));
        }
      }

      for (Map.Entry<String, ValueProto.RepeatedValue> entityEntry :
          response.getEntitiesMap().entrySet()) {
        row.setEntity(entityEntry.getKey(), entityEntry.getValue().getVal(rowIdx));
      }

      results.add(row);
    }
    return results;
  }

  public List<RangeRow> getOnlineFeaturesRange(
      List<String> featureRefs,
      List<Row> rows,
      List<SortKeyFilterModel> sortKeyFilters,
      int limit,
      boolean reverseSortOrder) {
    GetOnlineFeaturesRangeRequest request =
        GetOnlineFeaturesRangeRequest.newBuilder()
            .setFeatures(ServingAPIProto.FeatureList.newBuilder().addAllVal(featureRefs).build())
            .putAllEntities(transposeEntitiesOntoColumns(rows))
            .addAllSortKeyFilters(
                sortKeyFilters.stream()
                    .map(SortKeyFilterModel::toProto)
                    .collect(Collectors.toList()))
            .setLimit(limit)
            .setReverseSortOrder(reverseSortOrder)
            .setIncludeMetadata(false)
            .build();
    return getOnlineFeaturesRange(request, rows);
  }

  /**
   * Get online features from Feast via feature reference(s) with range support.
   *
   * @param featureRefs List of string feature references to retrieve in the format {@code
   *     featureTable:feature}.
   * @param rows List of {@link Row} to select the entities to retrieve the features for.
   * @param sortKeyFilters List of field names to use for sorting the feature results.
   * @param limit Maximum number of results to return.
   * @param reverseSortOrder If true, the results will be returned in descending order.
   * @param project The Feast project to retrieve features from.
   * @return List of {@link RangeRow} containing retrieved data fields.
   */
  public List<RangeRow> getOnlineFeaturesRange(
      List<String> featureRefs,
      List<Row> rows,
      List<SortKeyFilterModel> sortKeyFilters,
      int limit,
      boolean reverseSortOrder,
      String project) {
    GetOnlineFeaturesRangeRequest request =
        GetOnlineFeaturesRangeRequest.newBuilder()
            .setFeatures(ServingAPIProto.FeatureList.newBuilder().addAllVal(featureRefs).build())
            .putAllEntities(transposeEntitiesOntoColumns(rows))
            .addAllSortKeyFilters(
                sortKeyFilters.stream()
                    .map(SortKeyFilterModel::toProto)
                    .collect(Collectors.toList()))
            .setLimit(limit)
            .setReverseSortOrder(reverseSortOrder)
            .setIncludeMetadata(false)
            .build();
    return getOnlineFeaturesRange(request, rows);
  }

  public List<RangeRow> getOnlineFeaturesRange(
      List<String> featureRefs,
      List<Row> rows,
      List<SortKeyFilterModel> sortKeyFilters,
      int limit,
      boolean reverseSortOrder,
      String project,
      boolean includeMetadata) {
    GetOnlineFeaturesRangeRequest request =
        GetOnlineFeaturesRangeRequest.newBuilder()
            .setFeatures(ServingAPIProto.FeatureList.newBuilder().addAllVal(featureRefs).build())
            .putAllEntities(transposeEntitiesOntoColumns(rows))
            .addAllSortKeyFilters(
                sortKeyFilters.stream()
                    .map(SortKeyFilterModel::toProto)
                    .collect(Collectors.toList()))
            .setLimit(limit)
            .setReverseSortOrder(reverseSortOrder)
            .setIncludeMetadata(includeMetadata)
            .build();
    return getOnlineFeaturesRange(request, rows);
  }

  protected FeastClient(ManagedChannel channel, Optional<CallCredentials> credentials) {
    this(channel, credentials, 0);
  }

  protected FeastClient(
      ManagedChannel channel, Optional<CallCredentials> credentials, long requestTimeout) {
    this.channel = channel;
    this.requestTimeout = requestTimeout;
    TracingClientInterceptor tracingInterceptor =
        TracingClientInterceptor.newBuilder().withTracer(GlobalTracer.get()).build();

    ServingServiceBlockingStub servingStub =
        ServingServiceGrpc.newBlockingStub(tracingInterceptor.intercept(channel));

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
