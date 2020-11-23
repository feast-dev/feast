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
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

import com.google.common.collect.ImmutableMap;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import feast.common.it.DataGenerator;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureTableProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import io.grpc.ManagedChannel;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runners.model.InitializationError;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.core-authentication.enabled=true",
      "feast.core-authentication.provider=oauth",
      "feast.security.authentication.enabled=true",
      "feast.security.authorization.enabled=false"
    })
@Testcontainers
public class ServingServiceOauthAuthenticationIT extends BaseAuthIT {

  CoreSimpleAPIClient coreClient;
  FeatureTableProto.FeatureTableSpec expectedFeatureTableSpec;
  static final Map<String, String> options = new HashMap<>();

  static final int FEAST_SERVING_PORT = 6566;
  @LocalServerPort private int metricsPort;

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-it-hydra.yml"),
              new File("src/test/resources/docker-compose/docker-compose-it.yml"))
          .withExposedService(HYDRA, HYDRA_PORT, forHttp("/health/alive").forStatusCode(200))
          .withExposedService(
              CORE,
              FEAST_CORE_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)));

  @BeforeAll
  static void globalSetup() throws IOException, InitializationError, InterruptedException {
    String hydraExternalHost = environment.getServiceHost(HYDRA, HYDRA_PORT);
    Integer hydraExternalPort = environment.getServicePort(HYDRA, HYDRA_PORT);
    String hydraExternalUrl = String.format("http://%s:%s", hydraExternalHost, hydraExternalPort);
    AuthTestUtils.seedHydra(hydraExternalUrl, CLIENT_ID, CLIENT_SECRET, AUDIENCE, GRANT_TYPE);

    // set up options for call credentials
    options.put("oauth_url", TOKEN_URL);
    options.put(CLIENT_ID, CLIENT_ID);
    options.put(CLIENT_SECRET, CLIENT_SECRET);
    options.put("jwkEndpointURI", JWK_URI);
    options.put("audience", AUDIENCE);
    options.put("grant_type", GRANT_TYPE);
  }

  @BeforeEach
  public void initState() {
    coreClient = AuthTestUtils.getSecureApiClientForCore(FEAST_CORE_PORT, options);
    EntityProto.EntitySpecV2 entitySpec =
        DataGenerator.createEntitySpecV2(
            ENTITY_ID,
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));
    coreClient.simpleApplyEntity(PROJECT_NAME, entitySpec);

    expectedFeatureTableSpec =
        DataGenerator.createFeatureTableSpec(
                FEATURE_TABLE_NAME,
                Arrays.asList(ENTITY_ID),
                new HashMap<>() {
                  {
                    put(FEATURE_NAME, ValueProto.ValueType.Enum.STRING);
                  }
                },
                7200,
                ImmutableMap.of("feat_key2", "feat_value2"))
            .toBuilder()
            .setBatchSource(
                DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
            .build();
    coreClient.simpleApplyFeatureTable(PROJECT_NAME, expectedFeatureTableSpec);
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
  public void shouldAllowUnauthenticatedGetOnlineFeatures() {
    FeatureTableProto.FeatureTable actualFeatureTable =
        coreClient.simpleGetFeatureTable(PROJECT_NAME, FEATURE_TABLE_NAME);
    assertEquals(expectedFeatureTableSpec.getName(), actualFeatureTable.getSpec().getName());
    assertEquals(
        expectedFeatureTableSpec.getBatchSource(), actualFeatureTable.getSpec().getBatchSource());

    ServingServiceBlockingStub servingStub =
        AuthTestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
    GetOnlineFeaturesRequestV2 onlineFeatureRequestV2 =
        AuthTestUtils.createOnlineFeatureRequest(
            PROJECT_NAME, FEATURE_TABLE_NAME, FEATURE_NAME, ENTITY_ID, 1);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequestV2);

    assertEquals(1, featureResponse.getFieldValuesCount());
    Map<String, Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
    assertTrue(fieldsMap.containsKey(ENTITY_ID));
    assertTrue(fieldsMap.containsKey(FEATURE_TABLE_NAME + ":" + FEATURE_NAME));
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }

  @Test
  void canGetOnlineFeaturesIfAuthenticated() {
    FeatureTableProto.FeatureTable actualFeatureTable =
        coreClient.simpleGetFeatureTable(PROJECT_NAME, FEATURE_TABLE_NAME);
    assertEquals(expectedFeatureTableSpec.getName(), actualFeatureTable.getSpec().getName());
    assertEquals(
        expectedFeatureTableSpec.getBatchSource(), actualFeatureTable.getSpec().getBatchSource());

    ServingServiceBlockingStub servingStub =
        AuthTestUtils.getServingServiceStub(true, FEAST_SERVING_PORT, options);
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        AuthTestUtils.createOnlineFeatureRequest(
            PROJECT_NAME, FEATURE_TABLE_NAME, FEATURE_NAME, ENTITY_ID, 1);

    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);
    assertEquals(1, featureResponse.getFieldValuesCount());
    Map<String, Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
    assertTrue(fieldsMap.containsKey(ENTITY_ID));
    assertTrue(fieldsMap.containsKey(FEATURE_TABLE_NAME + ":" + FEATURE_NAME));
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }
}
