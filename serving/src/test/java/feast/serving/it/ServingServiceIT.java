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

import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import feast.common.models.FeatureV2;
import feast.proto.core.EntityProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import io.grpc.ManagedChannel;
import io.lettuce.core.KeyValue;
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
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runners.model.InitializationError;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class ServingServiceIT extends BaseAuthIT {

  static final Map<String, String> options = new HashMap<>();
  static final String timestampPrefix = "_ts";
  static CoreSimpleAPIClient coreClient;
  static RedisCommands<byte[], byte[]> syncCommands;

  // To decode bytes back to Feature Reference
  static Map<String, ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap =
      new HashMap<>();
  // To check whether redis ValueK is a timestamp field
  static Map<String, Boolean> isTimestampMap = new HashMap<>();
  static List<List<byte[]>> featureReferenceWithTsByteArrays = new ArrayList<>();
  static RedisProto.RedisKeyV2 redisKey = null;

  static final int FEAST_SERVING_PORT = 6566;
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
          .withExposedService(
              JOB_CONTROLLER,
              FEAST_JOB_CONTROLLER_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)))
          .withExposedService(REDIS, REDIS_PORT);

  @BeforeAll
  static void globalSetup() throws IOException, InitializationError, InterruptedException {
    // Create Core client
    coreClient = TestUtils.getApiClientForCore(FEAST_CORE_PORT);

    // Create Redis client
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
    List<String> entities =
        new ArrayList<>() {
          {
            add(entityName);
          }
        };
    HashMap<String, ValueProto.ValueType.Enum> features = new HashMap<>();

    // Feature 1
    String feature1 = "trip_cost";
    ValueProto.Value feature1Value = ValueProto.Value.newBuilder().setDoubleVal(42.2).build();
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature1)
            .build();

    // Feature 2
    String feature2 = "trip_distance";
    ValueProto.Value feature2Value = ValueProto.Value.newBuilder().setInt64Val(42).build();
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature2)
            .build();

    // Feature 2
    String feature3 = "trip_empty";
    ValueProto.Value feature3Value = ValueProto.Value.newBuilder().build();
    ServingAPIProto.FeatureReferenceV2 feature3Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature3)
            .build();

    // Event Timestamp
    String eventTimestampKey = timestampPrefix + ":" + featureTableName;
    Timestamp eventTimestampValue = Timestamp.newBuilder().setSeconds(100).build();

    features.put(feature1, ValueProto.ValueType.Enum.INT64);
    features.put(feature2, ValueProto.ValueType.Enum.DOUBLE);
    features.put(feature3, ValueProto.ValueType.Enum.DOUBLE);

    TestUtils.applyFeatureTable(coreClient, projectName, featureTableName, entities, features);

    // Serialize Redis Key with Entity i.e <default_driver_id_1>
    redisKey =
        RedisProto.RedisKeyV2.newBuilder()
            .setProject(projectName)
            .addEntityNames(entityName)
            .addEntityValues(entityValue)
            .build();

    // Murmur hash Redis Value Field i.e murmur(<rides:trip_distance>)
    Map<ServingAPIProto.FeatureReferenceV2, ValueProto.Value> featureReferenceValueMap =
        new HashMap<>() {
          {
            put(feature1Reference, feature1Value);
            put(feature2Reference, feature2Value);
            put(feature3Reference, feature3Value);
          }
        };

    // Insert timestamp into Redis and isTimestampMap only once
    syncCommands.hset(
        redisKey.toByteArray(), eventTimestampKey.getBytes(), eventTimestampValue.toByteArray());
    isTimestampMap.put(Arrays.toString(eventTimestampKey.getBytes()), true);
    featureReferenceValueMap.forEach(
        (featureReference, featureValue) -> {
          List<byte[]> currentFeatureWithTsByteArrays = new ArrayList<>();
          String delimitedFeatureReference =
              featureReference.getFeatureTable() + ":" + featureReference.getName();
          byte[] featureReferenceBytes =
              Hashing.murmur3_32()
                  .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
                  .asBytes();
          // Insert features into Redis
          syncCommands.hset(
              redisKey.toByteArray(), featureReferenceBytes, featureValue.toByteArray());
          currentFeatureWithTsByteArrays.add(featureReferenceBytes);
          isTimestampMap.put(Arrays.toString(featureReferenceBytes), false);
          byteToFeatureReferenceMap.put(featureReferenceBytes.toString(), featureReference);

          currentFeatureWithTsByteArrays.add(eventTimestampKey.getBytes());

          featureReferenceWithTsByteArrays.add(currentFeatureWithTsByteArrays);
        });

    // set up options for call credentials
    options.put("oauth_url", TOKEN_URL);
    options.put(CLIENT_ID, CLIENT_ID);
    options.put(CLIENT_SECRET, CLIENT_SECRET);
    options.put("jwkEndpointURI", JWK_URI);
    options.put("audience", AUDIENCE);
    options.put("grant_type", GRANT_TYPE);
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

  //  @Test
  //  public void shouldAllowUnauthenticatedGetOnlineFeatures() {
  //    // apply feature set
  //    CoreSimpleAPIClient coreClient =
  //        TestUtils.getSecureApiClientForCore(FEAST_CORE_PORT, options);
  //    TestUtils.applyFeatureTable(coreClient, PROJECT_NAME, ENTITY_ID, FEATURE_NAME);
  //    ServingServiceGrpc.ServingServiceBlockingStub servingStub =
  //        TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
  //
  //    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
  //        TestUtils.createOnlineFeatureRequest(PROJECT_NAME, FEATURE_NAME, ENTITY_ID, 1);
  //    GetOnlineFeaturesResponse featureResponse =
  //        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);
  //    assertEquals(1, featureResponse.getFieldValuesCount());
  //    Map<String, ValueProto.Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
  //
  //    assertTrue(fieldsMap.containsKey(ENTITY_ID));
  //    assertTrue(fieldsMap.containsKey(FEATURE_NAME));
  //
  //    ((ManagedChannel) servingStub.getChannel()).shutdown();
  //  }

  @Test
  public void shouldRegisterAndRetrieveFromRedis() throws InvalidProtocolBufferException {
    String featureTableName = "rides";

    // Feature 1
    String feature1 = "trip_cost";
    ValueProto.Value feature1Value = ValueProto.Value.newBuilder().setDoubleVal(42.2).build();
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature1)
            .build();

    // Feature 2
    String feature2 = "trip_distance";
    ValueProto.Value feature2Value = ValueProto.Value.newBuilder().setInt64Val(42).build();
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature2)
            .build();

    // Feature 2
    String feature3 = "trip_empty";
    ValueProto.Value feature3Value = ValueProto.Value.newBuilder().build();
    ServingAPIProto.FeatureReferenceV2 feature3Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature3)
            .build();

    // Retrieve multiple value from Redis
    List<Optional<Feature>> retrievedFeatures = new ArrayList<>();
    for (List<byte[]> currentFeatureReferenceWithTs : featureReferenceWithTsByteArrays) {
      ServingAPIProto.FeatureReferenceV2 featureReference = null;
      ValueProto.Value featureValue = null;
      Timestamp eventTimestamp = null;

      List<KeyValue<byte[], byte[]>> currentRedisValuesList =
          syncCommands.hmget(
              redisKey.toByteArray(),
              currentFeatureReferenceWithTs.get(0),
              currentFeatureReferenceWithTs.get(1));

      for (int i = 0; i < currentRedisValuesList.size(); i++) {
        if (currentRedisValuesList.get(i).hasValue()) {
          try {
            byte[] redisValueK = currentRedisValuesList.get(i).getKey();
            byte[] redisValueV = currentRedisValuesList.get(i).getValue();

            // Decode data from Redis into Feature object fields
            if (isTimestampMap.get(Arrays.toString(redisValueK))) {
              eventTimestamp = Timestamp.parseFrom(redisValueV);
            } else {
              featureReference = byteToFeatureReferenceMap.get(redisValueK.toString());
              featureValue = ValueProto.Value.parseFrom(redisValueV);
            }
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
        }
      }
      // Check for null featureReference i.e key is not found
      if (featureReference != null) {
        Feature feature =
            Feature.builder()
                .setFeatureReference(featureReference)
                .setFeatureValue(featureValue)
                .setEventTimestamp(eventTimestamp)
                .build();
        retrievedFeatures.add(Optional.of(feature));
      }
    }

    List<ValueProto.Value> expectedValues =
        new ArrayList<>() {
          {
            add(feature1Value);
            add(feature2Value);
            add(feature3Value);
          }
        };
    List<ServingAPIProto.FeatureReferenceV2> expectedFeatureReferences =
        new ArrayList<>() {
          {
            add(feature1Reference);
            add(feature2Reference);
            add(feature3Reference);
          }
        };
    assertEquals(3, retrievedFeatures.size());
    assertEquals(
        expectedFeatureReferences.stream()
            .map(featureReference -> featureReference.getName())
            .sorted()
            .collect(Collectors.toList()),
        retrievedFeatures.stream()
            .map(feature -> feature.get().getFeatureReference().getName())
            .sorted()
            .collect(Collectors.toList()));
    assertEquals(
        expectedFeatureReferences.stream()
            .map(featureReference -> featureReference.getFeatureTable())
            .collect(Collectors.toList()),
        retrievedFeatures.stream()
            .map(feature -> feature.get().getFeatureReference().getFeatureTable())
            .collect(Collectors.toList()));
  }

  @Test
  public void shouldRegisterAndGetOnlineFeatures() {
    ServingServiceGrpc.ServingServiceBlockingStub servingStub =
        TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    // getOnlineFeatures Information
    String projectName = "default";
    String featureTableName = "rides";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    String feature1 = "trip_cost";

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
            .setTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields(entityName, entityValue)
            .build();
    List<GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        new ArrayList<>() {
          {
            add(entityRow1);
          }
        };

    // Instantiate FeatureReferences
    ValueProto.Value feature1Value = ValueProto.Value.newBuilder().setDoubleVal(42.2).build();
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature1)
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        new ArrayList<>() {
          {
            add(feature1Reference);
          }
        };

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    Map<String, ValueProto.Value> expectedValueMap =
        new HashMap<>() {
          {
            put(entityName, entityValue);
            put(FeatureV2.getFeatureStringRef(feature1Reference), feature1Value);
          }
        };

    Map<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        new HashMap<>() {
          {
            put(entityName, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
            put(
                FeatureV2.getFeatureStringRef(feature1Reference),
                GetOnlineFeaturesResponse.FieldStatus.PRESENT);
          }
        };

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    List<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        new ArrayList<>() {
          {
            add(expectedFieldValues);
          }
        };

    assertEquals(1, 1);
    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());

    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }

  @Test
  public void shouldRegisterAndGetOnlineFeaturesWithNotFound() {
    ServingServiceGrpc.ServingServiceBlockingStub servingStub =
        TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    // getOnlineFeatures Information
    String projectName = "default";
    String featureTableName = "rides";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    String feature1 = "trip_cost";
    String notFoundFeature = "trip_transaction";
    String emptyFeature = "trip_empty";

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
            .setTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields(entityName, entityValue)
            .build();
    List<GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        new ArrayList<>() {
          {
            add(entityRow1);
          }
        };

    // Instantiate FeatureReferences
    ValueProto.Value featureValue = ValueProto.Value.newBuilder().setDoubleVal(42.2).build();
    ServingAPIProto.FeatureReferenceV2 featureReference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(feature1)
            .build();
    ValueProto.Value notFoundFeatureValue = ValueProto.Value.newBuilder().build();
    ServingAPIProto.FeatureReferenceV2 notFoundFeatureReference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(notFoundFeature)
            .build();
    ValueProto.Value emptyFeatureValue = ValueProto.Value.newBuilder().build();
    ServingAPIProto.FeatureReferenceV2 emptyFeatureReference =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable(featureTableName)
            .setName(emptyFeature)
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        new ArrayList<>() {
          {
            add(featureReference);
            add(notFoundFeatureReference);
            add(emptyFeatureReference);
          }
        };

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    Map<String, ValueProto.Value> expectedValueMap =
        new HashMap<>() {
          {
            put(entityName, entityValue);
            put(FeatureV2.getFeatureStringRef(featureReference), featureValue);
            put(FeatureV2.getFeatureStringRef(notFoundFeatureReference), notFoundFeatureValue);
            put(FeatureV2.getFeatureStringRef(emptyFeatureReference), emptyFeatureValue);
          }
        };

    Map<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        new HashMap<>() {
          {
            put(entityName, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
            put(
                FeatureV2.getFeatureStringRef(featureReference),
                GetOnlineFeaturesResponse.FieldStatus.PRESENT);
            put(
                FeatureV2.getFeatureStringRef(notFoundFeatureReference),
                GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND);
            put(
                FeatureV2.getFeatureStringRef(emptyFeatureReference),
                GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE);
          }
        };

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    List<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        new ArrayList<>() {
          {
            add(expectedFieldValues);
          }
        };

    assertEquals(1, 1);
    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());

    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }
}
