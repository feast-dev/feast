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

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.protobuf.Timestamp;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import feast.common.it.DataGenerator;
import feast.common.models.FeatureV2;
import feast.proto.core.EntityProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.core-cache-refresh-interval=1",
    })
@Testcontainers
public class ServingServiceIT extends BaseAuthIT {

  static final Map<String, String> options = new HashMap<>();
  static final String timestampPrefix = "_ts";
  static CoreSimpleAPIClient coreClient;
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;
  static RedisCommands<byte[], byte[]> syncCommands;

  static final int FEAST_SERVING_PORT = 6568;
  @LocalServerPort private int metricsPort;

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-it.yml"))
          .withExposedService(
              CORE,
              FEAST_CORE_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)))
          .withExposedService(REDIS, REDIS_PORT);

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() {
    coreClient = TestUtils.getApiClientForCore(FEAST_CORE_PORT);
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    RedisClient redisClient =
        RedisClient.create(
            new RedisURI(
                environment.getServiceHost("redis_1", REDIS_PORT),
                environment.getServicePort("redis_1", REDIS_PORT),
                java.time.Duration.ofMillis(2000)));
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    syncCommands = connection.sync();

    String projectName = "default";
    // Apply Entity
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    String description = "My driver id";
    ValueProto.ValueType.Enum entityType = ValueProto.ValueType.Enum.INT64;
    EntityProto.EntitySpecV2 entitySpec =
        EntityProto.EntitySpecV2.newBuilder()
            .setName(entityName)
            .setDescription(description)
            .setValueType(entityType)
            .build();
    TestUtils.applyEntity(coreClient, projectName, entitySpec);

    // Apply FeatureTable
    String featureTableName = "rides";
    ImmutableList<String> entities = ImmutableList.of(entityName);

    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        DataGenerator.createFeatureReference("rides", "trip_distance");
    ServingAPIProto.FeatureReferenceV2 feature3Reference =
        DataGenerator.createFeatureReference("rides", "trip_empty");
    ServingAPIProto.FeatureReferenceV2 feature4Reference =
        DataGenerator.createFeatureReference("rides", "trip_wrong_type");

    // Event Timestamp
    String eventTimestampKey = timestampPrefix + ":" + featureTableName;
    Timestamp eventTimestampValue = Timestamp.newBuilder().setSeconds(100).build();

    ImmutableMap<String, ValueProto.ValueType.Enum> features =
        ImmutableMap.of(
            "trip_cost",
            ValueProto.ValueType.Enum.INT64,
            "trip_distance",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_empty",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_wrong_type",
            ValueProto.ValueType.Enum.STRING);

    TestUtils.applyFeatureTable(
        coreClient, projectName, featureTableName, entities, features, 7200);

    // Serialize Redis Key with Entity i.e <default_driver_id_1>
    RedisProto.RedisKeyV2 redisKey =
        RedisProto.RedisKeyV2.newBuilder()
            .setProject(projectName)
            .addEntityNames(entityName)
            .addEntityValues(entityValue)
            .build();

    ImmutableMap<ServingAPIProto.FeatureReferenceV2, ValueProto.Value> featureReferenceValueMap =
        ImmutableMap.of(
            feature1Reference,
            DataGenerator.createInt64Value(42),
            feature2Reference,
            DataGenerator.createDoubleValue(42.2),
            feature3Reference,
            DataGenerator.createEmptyValue(),
            feature4Reference,
            DataGenerator.createDoubleValue(42.2));

    // Insert timestamp into Redis and isTimestampMap only once
    syncCommands.hset(
        redisKey.toByteArray(), eventTimestampKey.getBytes(), eventTimestampValue.toByteArray());
    featureReferenceValueMap.forEach(
        (featureReference, featureValue) -> {
          // Murmur hash Redis Feature Field i.e murmur(<rides:trip_distance>)
          String delimitedFeatureReference =
              featureReference.getFeatureTable() + ":" + featureReference.getName();
          byte[] featureReferenceBytes =
              Hashing.murmur3_32()
                  .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
                  .asBytes();
          // Insert features into Redis
          syncCommands.hset(
              redisKey.toByteArray(), featureReferenceBytes, featureValue.toByteArray());
        });

    // set up options for call credentials
    options.put("oauth_url", TOKEN_URL);
    options.put(CLIENT_ID, CLIENT_ID);
    options.put(CLIENT_SECRET, CLIENT_SECRET);
    options.put("jwkEndpointURI", JWK_URI);
    options.put("audience", AUDIENCE);
    options.put("grant_type", GRANT_TYPE);
  }

  @AfterAll
  static void tearDown() {
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }

  /** Test that Feast Serving metrics endpoint can be accessed with authentication enabled */
  @Test
  public void shouldAllowUnauthenticatedAccessToMetricsEndpoint() throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("http://localhost:%d/metrics", metricsPort))
            .get()
            .build();
    Response response = new OkHttpClient().newCall(request).execute();
    assertTrue(response.isSuccessful());
    assertTrue(!response.body().string().isEmpty());
  }

  @Test
  public void shouldRegisterAndGetOnlineFeatures() {
    // getOnlineFeatures Information
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, DataGenerator.createInt64Value(1), 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(feature1Reference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(feature1Reference),
            DataGenerator.createInt64Value(42));

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(feature1Reference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldRegisterAndGetOnlineFeaturesWithNotFound() {
    // getOnlineFeatures Information
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, DataGenerator.createInt64Value(1), 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    ServingAPIProto.FeatureReferenceV2 notFoundFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_transaction");
    ServingAPIProto.FeatureReferenceV2 emptyFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_empty");

    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference, notFoundFeatureReference, emptyFeatureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt64Value(42),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue(),
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND,
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldGetOnlineFeaturesOutsideMaxAge() {
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, DataGenerator.createInt64Value(1), 7400);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");

    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldReturnNotFoundForDiffType() {
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, DataGenerator.createInt64Value(1), 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_wrong_type");

    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldReturnNotFoundForUpdatedType() {
    String projectName = "default";
    String entityName = "driver_id";
    String featureTableName = "rides";

    ImmutableList<String> entities = ImmutableList.of(entityName);
    ImmutableMap<String, ValueProto.ValueType.Enum> features =
        ImmutableMap.of(
            "trip_cost",
            ValueProto.ValueType.Enum.INT64,
            "trip_distance",
            ValueProto.ValueType.Enum.STRING,
            "trip_empty",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_wrong_type",
            ValueProto.ValueType.Enum.STRING);

    TestUtils.applyFeatureTable(
        coreClient, projectName, featureTableName, entities, features, 7200);

    // Sleep is necessary to ensure caching (every 1s) of updated FeatureTable is done
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
    }

    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, DataGenerator.createInt64Value(1), 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_distance");

    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }
}
