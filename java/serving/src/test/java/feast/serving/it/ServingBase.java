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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import feast.serving.util.DataGenerator;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
abstract class ServingBase {
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;

  static final int FEAST_SERVING_PORT = 6568;

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);

    registry.add("feast.stores[0].name", () -> "online");
    registry.add("feast.stores[0].type", () -> "REDIS");
    registry.add("feast.stores[0].config.host", () -> environment.getServiceHost("redis", 6379));
    registry.add("feast.stores[0].config.port", () -> environment.getServicePort("redis", 6379));
  }

  static DockerComposeContainer environment;

  static {
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

  @BeforeAll
  static void globalSetup() {
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
  }

  @AfterAll
  static void tearDown() throws Exception {
    ((ManagedChannel) servingStub.getChannel()).shutdown().awaitTermination(10, TimeUnit.SECONDS);
  }

  protected ServingAPIProto.GetOnlineFeaturesRequestV2 buildOnlineRequest(int driverId) {
    // getOnlineFeatures Information
    String projectName = "feast_project";
    String entityName = "driver_id";

    // Instantiate EntityRows
    final Timestamp timestamp = Timestamp.getDefaultInstance();
    ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(
            entityName, DataGenerator.createInt64Value(driverId), timestamp.getSeconds());
    ImmutableList<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        ImmutableList.of(entityRow1);

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
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(buildOnlineRequest(1005));

    assertEquals(1, featureResponse.getFieldValuesCount());

    final ServingAPIProto.GetOnlineFeaturesResponse.FieldValues fieldValue =
        featureResponse.getFieldValues(0);
    for (final String key :
        ImmutableList.of(
            "driver_hourly_stats:avg_daily_trips", "driver_hourly_stats:conv_rate", "driver_id")) {
      assertTrue(fieldValue.containsFields(key));
      assertTrue(fieldValue.containsStatuses(key));
      assertEquals(
          ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus.PRESENT,
          fieldValue.getStatusesOrThrow(key));
    }

    assertEquals(
        500, fieldValue.getFieldsOrThrow("driver_hourly_stats:avg_daily_trips").getInt64Val());
    assertEquals(1005, fieldValue.getFieldsOrThrow("driver_id").getInt64Val());
    assertEquals(
        0.5, fieldValue.getFieldsOrThrow("driver_hourly_stats:conv_rate").getDoubleVal(), 0.0001);
  }

  @Test
  @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
  public void shouldGetOnlineFeaturesWithOutsideMaxAgeStatus() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(buildOnlineRequest(1001));

    assertEquals(1, featureResponse.getFieldValuesCount());

    final ServingAPIProto.GetOnlineFeaturesResponse.FieldValues fieldValue =
        featureResponse.getFieldValues(0);
    for (final String key :
        ImmutableList.of("driver_hourly_stats:avg_daily_trips", "driver_hourly_stats:conv_rate")) {
      assertTrue(fieldValue.containsFields(key));
      assertTrue(fieldValue.containsStatuses(key));
      assertEquals(
          ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE,
          fieldValue.getStatusesOrThrow(key));
    }

    assertEquals(
        100, fieldValue.getFieldsOrThrow("driver_hourly_stats:avg_daily_trips").getInt64Val());
    assertEquals(1001, fieldValue.getFieldsOrThrow("driver_id").getInt64Val());
    assertEquals(
        0.1, fieldValue.getFieldsOrThrow("driver_hourly_stats:conv_rate").getDoubleVal(), 0.0001);
  }

  @Test
  public void shouldGetOnlineFeaturesWithNotFoundStatus() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(buildOnlineRequest(-1));

    assertEquals(1, featureResponse.getFieldValuesCount());

    final ServingAPIProto.GetOnlineFeaturesResponse.FieldValues fieldValue =
        featureResponse.getFieldValues(0);
    for (final String key :
        ImmutableList.of("driver_hourly_stats:avg_daily_trips", "driver_hourly_stats:conv_rate")) {
      assertTrue(fieldValue.containsFields(key));
      assertTrue(fieldValue.containsStatuses(key));
      assertEquals(
          ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND,
          fieldValue.getStatusesOrThrow(key));
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

    ServingAPIProto.GetOnlineFeaturesRequestV2 request =
        buildOnlineRequest(1005)
            .toBuilder()
            .addFeatures(DataGenerator.createFeatureReference("new_view", "new_feature"))
            .build();

    await()
        .ignoreException(StatusRuntimeException.class)
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> servingStub.getOnlineFeaturesV2(request).getFieldValuesCount(), equalTo(1));
  }

  abstract void updateRegistryFile(RegistryProto.Registry registry);
}
