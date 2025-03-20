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
package eg.feast;

import com.google.common.collect.Lists;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.ValueProto;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
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

    ServingServiceGrpc.ServingServiceBlockingStub timedStub =
        requestTimeout != 0 ? stub.withDeadlineAfter(requestTimeout, TimeUnit.MILLISECONDS) : stub;

    GetOnlineFeaturesResponse response = timedStub.getOnlineFeatures(requestBuilder.build());

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
