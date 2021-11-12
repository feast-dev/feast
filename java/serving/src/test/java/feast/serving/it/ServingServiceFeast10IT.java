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
package feast.serving.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import feast.common.it.DataGenerator;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import io.grpc.ManagedChannel;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.registry:src/test/resources/docker-compose/feast10/registry.db",
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@Testcontainers
public class ServingServiceFeast10IT extends BaseAuthIT {

  public static final Logger log = LoggerFactory.getLogger(ServingServiceFeast10IT.class);

  static final String timestampPrefix = "_ts";
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;

  static final int FEAST_SERVING_PORT = 6568;
  @LocalServerPort private int metricsPort;

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-feast10-it.yml"))
          .withExposedService(REDIS, REDIS_PORT);

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() {
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
  }

  @AfterAll
  static void tearDown() throws Exception {
    ((ManagedChannel) servingStub.getChannel()).shutdown().awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
  public void shouldGetOnlineFeatures() {
    // getOnlineFeatures Information
    String projectName = "feast_project";
    String entityName = "driver_id";

    // Instantiate EntityRows
    final Timestamp timestamp = Timestamp.getDefaultInstance();
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(
            entityName, DataGenerator.createInt64Value(1001), timestamp.getSeconds());
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "conv_rate");
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "avg_daily_trips");
    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(feature1Reference, feature2Reference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    assertEquals(1, featureResponse.getFieldValuesCount());

    final GetOnlineFeaturesResponse.FieldValues fieldValue = featureResponse.getFieldValues(0);
    for (final String key :
        ImmutableList.of(
            "driver_hourly_stats:avg_daily_trips", "driver_hourly_stats:conv_rate", "driver_id")) {
      assertTrue(fieldValue.containsFields(key));
      assertTrue(fieldValue.containsStatuses(key));
      assertEquals(
          GetOnlineFeaturesResponse.FieldStatus.PRESENT, fieldValue.getStatusesOrThrow(key));
    }

    assertEquals(
        721, fieldValue.getFieldsOrThrow("driver_hourly_stats:avg_daily_trips").getInt64Val());
    assertEquals(1001, fieldValue.getFieldsOrThrow("driver_id").getInt64Val());
    assertEquals(
        0.74203354,
        fieldValue.getFieldsOrThrow("driver_hourly_stats:conv_rate").getDoubleVal(),
        0.0001);
  }
}
