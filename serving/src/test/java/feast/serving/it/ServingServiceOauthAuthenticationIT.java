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

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.ValueProto.Value;
import io.grpc.ManagedChannel;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
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

  static final Map<String, String> options = new HashMap<>();

  static final int FEAST_SERVING_PORT = 6566;
  @LocalServerPort private int metricsPort;

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-it-hydra.yml"),
              new File("src/test/resources/docker-compose/docker-compose-it-core.yml"))
          .withExposedService(HYDRA, HYDRA_PORT, forHttp("/health/alive").forStatusCode(200))
          .withExposedService(
              CORE,
              6565,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(CORE_START_MAX_WAIT_TIME_IN_MINUTES)));

  @BeforeAll
  static void globalSetup() throws IOException, InitializationError, InterruptedException {
    String hydraExternalHost = environment.getServiceHost(HYDRA, HYDRA_PORT);
    Integer hydraExternalPort = environment.getServicePort(HYDRA, HYDRA_PORT);
    String hydraExternalUrl = String.format("http://%s:%s", hydraExternalHost, hydraExternalPort);
    IntegrationTestUtils.seedHydra(
        hydraExternalUrl, CLIENT_ID, CLIENT_SECRET, AUDIENCE, GRANT_TYPE);

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

  @Test
  public void shouldAllowUnauthenticatedGetOnlineFeatures() {
    // apply feature set
    CoreSimpleAPIClient coreClient =
        IntegrationTestUtils.getSecureApiClientForCore(FEAST_CORE_PORT, options);
    IntegrationTestUtils.applyFeatureSet(
        coreClient, PROJECT_NAME, "test_1", ENTITY_ID, FEATURE_NAME, null);
    ServingServiceBlockingStub servingStub =
        IntegrationTestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
    GetOnlineFeaturesRequest onlineFeatureRequest =
        IntegrationTestUtils.createOnlineFeatureRequest(PROJECT_NAME, FEATURE_NAME, ENTITY_ID, 1);
    GetOnlineFeaturesResponse featureResponse = servingStub.getOnlineFeatures(onlineFeatureRequest);
    assertEquals(1, featureResponse.getFieldValuesCount());
    Map<String, Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
    assertTrue(fieldsMap.containsKey(ENTITY_ID));
    assertTrue(fieldsMap.containsKey(FEATURE_NAME));
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }

  @Test
  void canGetOnlineFeaturesIfAuthenticated() {
    // apply feature set
    CoreSimpleAPIClient coreClient =
        IntegrationTestUtils.getSecureApiClientForCore(FEAST_CORE_PORT, options);
    IntegrationTestUtils.applyFeatureSet(
        coreClient, PROJECT_NAME, "test_1", ENTITY_ID, FEATURE_NAME, null);
    ServingServiceBlockingStub servingStub =
        IntegrationTestUtils.getServingServiceStub(true, FEAST_SERVING_PORT, options);
    GetOnlineFeaturesRequest onlineFeatureRequest =
        IntegrationTestUtils.createOnlineFeatureRequest(PROJECT_NAME, FEATURE_NAME, ENTITY_ID, 1);
    GetOnlineFeaturesResponse featureResponse = servingStub.getOnlineFeatures(onlineFeatureRequest);
    assertEquals(1, featureResponse.getFieldValuesCount());
    Map<String, Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
    assertTrue(fieldsMap.containsKey(ENTITY_ID));
    assertTrue(fieldsMap.containsKey(FEATURE_NAME));
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }
}
