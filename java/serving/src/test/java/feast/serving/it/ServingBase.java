/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving.it;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.inject.*;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.google.common.collect.ImmutableMap;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import feast.serving.config.*;
import feast.serving.grpc.OnlineServingGrpcServiceV2;
import feast.serving.util.DataGenerator;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.util.MutableHandlerRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
abstract class ServingBase {
  static DockerComposeContainer environment;

  ServingServiceGrpc.ServingServiceBlockingStub servingStub;
  Injector injector;
  String serverName;
  ManagedChannel channel;
  Server server;
  MutableHandlerRegistry serviceRegistry;

  @BeforeAll
  static void globalSetup() {
    environment =
        new DockerComposeContainer(
                new File("src/test/resources/docker-compose/docker-compose-redis-it.yml"))
            .withExposedService("redis", 6379)
            .withOptions()
            .waitingFor(
                "materialize",
                Wait.forLogMessage(".*Materialization finished.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(5)));
    environment.start();
  }

  @AfterAll
  static void globalTeardown() {
    environment.stop();
  }

  @BeforeEach
  public void envSetUp() throws Exception {

    AbstractModule appPropertiesModule =
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(OnlineServingGrpcServiceV2.class);
          }

          @Provides
          ApplicationProperties applicationProperties() {
            final ApplicationProperties p = new ApplicationProperties();
            p.setAwsRegion("us-east-1");

            final ApplicationProperties.FeastProperties feastProperties = createFeastProperties();
            p.setFeast(feastProperties);

            final ApplicationProperties.TracingProperties tracingProperties =
                new ApplicationProperties.TracingProperties();
            feastProperties.setTracing(tracingProperties);

            tracingProperties.setEnabled(false);
            return p;
          }
        };

    Module overrideConfig = registryConfig();
    Module registryConfig;
    if (overrideConfig != null) {
      registryConfig = Modules.override(new RegistryConfig()).with(registryConfig());
    } else {
      registryConfig = new RegistryConfig();
    }

    injector =
        Guice.createInjector(
            new ServingServiceConfigV2(),
            registryConfig,
            new InstrumentationConfig(),
            appPropertiesModule);

    OnlineServingGrpcServiceV2 onlineServingGrpcServiceV2 =
        injector.getInstance(OnlineServingGrpcServiceV2.class);

    serverName = InProcessServerBuilder.generateName();

    server =
        InProcessServerBuilder.forName(serverName)
            .fallbackHandlerRegistry(serviceRegistry)
            .addService(onlineServingGrpcServiceV2)
            .addService(ProtoReflectionService.newInstance())
            .build();
    server.start();

    channel = InProcessChannelBuilder.forName(serverName).usePlaintext().directExecutor().build();

    servingStub =
        ServingServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(1, TimeUnit.SECONDS)
            .withWaitForReady();
  }

  @AfterEach
  public void envTeardown() throws Exception {
    // assume channel and server are not null
    channel.shutdown();
    server.shutdown();
    // fail the test if cannot gracefully shutdown
    try {
      assert channel.awaitTermination(5, TimeUnit.SECONDS)
          : "channel cannot be gracefully shutdown";
      assert server.awaitTermination(5, TimeUnit.SECONDS) : "server cannot be gracefully shutdown";
    } finally {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  protected ServingAPIProto.GetOnlineFeaturesRequest buildOnlineRequest(int driverId) {
    // getOnlineFeatures Information
    String projectName = "feast_project";
    String entityName = "driver_id";

    // Instantiate EntityRows
    Map<String, ValueProto.RepeatedValue> entityRows =
        ImmutableMap.of(
            entityName,
            ValueProto.RepeatedValue.newBuilder()
                .addVal(DataGenerator.createInt64Value(driverId))
                .build());

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "conv_rate");
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "avg_daily_trips");
    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(feature1Reference, feature2Reference);

    // Build GetOnlineFeaturesRequestV2
    return TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
  }

  static RegistryProto.Registry registryProto = readLocalRegistry();

  private static RegistryProto.Registry readLocalRegistry() {
    try {
      return RegistryProto.Registry.parseFrom(
          Files.readAllBytes(Paths.get("src/test/resources/docker-compose/feast10/registry.db")));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Test
  public void shouldGetOnlineFeatures() {
    ServingAPIProto.GetOnlineFeaturesResponseV2 featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(1005));

    assertEquals(2, featureResponse.getResultsCount());
    assertEquals(1, featureResponse.getEntitiesCount());
    assertEquals(1, featureResponse.getResults(0).getValuesCount());

    assertEquals(
        ImmutableList.of("driver_hourly_stats:conv_rate", "driver_hourly_stats:avg_daily_trips"),
        featureResponse.getMetadata().getFeatureNames().getValList());
    assertEquals(featureResponse.getMetadata().getEntityNamesList(), ImmutableList.of("driver_id"));

    for (int featureIdx : List.of(0, 1)) {
      assertEquals(
          featureResponse.getResults(featureIdx).getStatusesList(),
          List.of(ServingAPIProto.FieldStatus.PRESENT));
    }

    assertEquals(1005, featureResponse.getEntities(0).getVal(0).getInt64Val());

    assertEquals(0.5, featureResponse.getResults(0).getValues(0).getDoubleVal(), 0.0001);
    assertEquals(500, featureResponse.getResults(1).getValues(0).getInt64Val());
  }

  @Test
  public void shouldGetOnlineFeaturesWithOutsideMaxAgeStatus() {
    ServingAPIProto.GetOnlineFeaturesResponseV2 featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(1001));

    assertEquals(2, featureResponse.getResultsCount());
    assertEquals(1, featureResponse.getEntitiesCount());
    assertEquals(1, featureResponse.getResults(0).getValuesCount());

    for (int featureIdx : List.of(0, 1)) {
      assertEquals(
          FieldStatus.OUTSIDE_MAX_AGE, featureResponse.getResults(featureIdx).getStatuses(0));
    }

    assertEquals(1001, featureResponse.getEntities(0).getVal(0).getInt64Val());

    assertEquals(0.1, featureResponse.getResults(0).getValues(0).getDoubleVal(), 0.0001);
    assertEquals(100, featureResponse.getResults(1).getValues(0).getInt64Val());
  }

  @Test
  public void shouldGetOnlineFeaturesWithNotFoundStatus() {
    ServingAPIProto.GetOnlineFeaturesResponseV2 featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(-1));

    assertEquals(2, featureResponse.getResultsCount());
    assertEquals(1, featureResponse.getEntitiesCount());
    assertEquals(1, featureResponse.getResults(0).getValuesCount());

    for (final int featureIdx : List.of(0, 1)) {
      assertEquals(FieldStatus.NOT_FOUND, featureResponse.getResults(featureIdx).getStatuses(0));
    }
  }

  @Test
  public void shouldRefreshRegistryAndServeNewFeatures() throws InterruptedException {
    updateRegistryFile(
        registryProto
            .toBuilder()
            .addFeatureViews(
                FeatureViewProto.FeatureView.newBuilder()
                    .setSpec(
                        FeatureViewProto.FeatureViewSpec.newBuilder()
                            .setName("new_view")
                            .addEntities("driver_id")
                            .addFeatures(
                                FeatureProto.FeatureSpecV2.newBuilder()
                                    .setName("new_feature")
                                    .setValueType(ValueProto.ValueType.Enum.BOOL))))
            .build());

    ServingAPIProto.GetOnlineFeaturesRequest request = buildOnlineRequest(1005);

    ServingAPIProto.GetOnlineFeaturesRequest requestWithNewFeature =
        request
            .toBuilder()
            .setFeatures(request.getFeatures().toBuilder().addVal("new_view:new_feature"))
            .build();

    await()
        .ignoreException(StatusRuntimeException.class)
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> servingStub.getOnlineFeatures(requestWithNewFeature).getResultsCount(),
            equalTo(3));
  }

  abstract ApplicationProperties.FeastProperties createFeastProperties();

  AbstractModule registryConfig() {
    return null;
  }

  abstract void updateRegistryFile(RegistryProto.Registry registry);
}
