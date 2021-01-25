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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

import feast.common.it.DataGenerator;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runners.model.InitializationError;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import sh.ory.keto.ApiException;

@ActiveProfiles("it")
@SpringBootTest(
    properties = {
      "feast.core-authentication.enabled=true",
      "feast.core-authentication.provider=oauth",
      "feast.security.authentication.enabled=true",
      "feast.security.authorization.enabled=true"
    })
@Testcontainers
public class ServingServiceOauthAuthorizationIT extends BaseAuthIT {

  static final Map<String, String> adminCredentials = new HashMap<>();
  static final Map<String, String> memberCredentials = new HashMap<>();
  static final String PROJECT_MEMBER_CLIENT_ID = "client_id_1";
  static final String NOT_PROJECT_MEMBER_CLIENT_ID = "client_id_2";
  private static int KETO_PORT = 4466;
  private static int KETO_ADAPTOR_PORT = 8080;
  static String subjectClaim = "sub";
  static CoreSimpleAPIClient coreClient;
  static final int FEAST_SERVING_PORT = 6766;

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-it-hydra.yml"),
              new File("src/test/resources/docker-compose/docker-compose-it.yml"),
              new File("src/test/resources/docker-compose/docker-compose-it-keto.yml"))
          .withExposedService(HYDRA, HYDRA_PORT, forHttp("/health/alive").forStatusCode(200))
          .withExposedService(
              CORE,
              FEAST_CORE_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)))
          .withExposedService("adaptor_1", KETO_ADAPTOR_PORT)
          .withExposedService("keto_1", KETO_PORT, forHttp("/health/ready").forStatusCode(200));

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {

    // Seed Keto with data
    String ketoExternalHost = environment.getServiceHost("keto_1", KETO_PORT);
    Integer ketoExternalPort = environment.getServicePort("keto_1", KETO_PORT);
    String ketoExternalUrl = String.format("http://%s:%s", ketoExternalHost, ketoExternalPort);
    try {
      AuthTestUtils.seedKeto(ketoExternalUrl, PROJECT_NAME, PROJECT_MEMBER_CLIENT_ID, CLIENT_ID);
    } catch (ApiException e) {
      throw new RuntimeException(String.format("Could not seed Keto store %s", ketoExternalUrl));
    }

    // Get Keto Authorization Server (Adaptor) url
    String ketoAdaptorHost = environment.getServiceHost("adaptor_1", KETO_ADAPTOR_PORT);
    Integer ketoAdaptorPort = environment.getServicePort("adaptor_1", KETO_ADAPTOR_PORT);
    String ketoAdaptorUrl = String.format("http://%s:%s", ketoAdaptorHost, ketoAdaptorPort);

    // Initialize dynamic properties
    registry.add("feast.security.authentication.options.subjectClaim", () -> subjectClaim);
    registry.add("feast.security.authentication.options.jwkEndpointURI", () -> JWK_URI);
    registry.add("feast.security.authorization.options.authorizationUrl", () -> ketoAdaptorUrl);
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() throws IOException, InitializationError, InterruptedException {
    String hydraExternalHost = environment.getServiceHost(HYDRA, HYDRA_PORT);
    Integer hydraExternalPort = environment.getServicePort(HYDRA, HYDRA_PORT);
    String hydraExternalUrl = String.format("http://%s:%s", hydraExternalHost, hydraExternalPort);
    AuthTestUtils.seedHydra(hydraExternalUrl, CLIENT_ID, CLIENT_SECRET, AUDIENCE, GRANT_TYPE);
    AuthTestUtils.seedHydra(
        hydraExternalUrl, PROJECT_MEMBER_CLIENT_ID, CLIENT_SECRET, AUDIENCE, GRANT_TYPE);
    AuthTestUtils.seedHydra(
        hydraExternalUrl, NOT_PROJECT_MEMBER_CLIENT_ID, CLIENT_SECRET, AUDIENCE, GRANT_TYPE);
    // set up options for call credentials
    adminCredentials.put("oauth_url", TOKEN_URL);
    adminCredentials.put(CLIENT_ID, CLIENT_ID);
    adminCredentials.put(CLIENT_SECRET, CLIENT_SECRET);
    adminCredentials.put("jwkEndpointURI", JWK_URI);
    adminCredentials.put("audience", AUDIENCE);
    adminCredentials.put("grant_type", GRANT_TYPE);

    coreClient = AuthTestUtils.getSecureApiClientForCore(FEAST_CORE_PORT, adminCredentials);
    coreClient.simpleApplyEntity(
        PROJECT_NAME,
        DataGenerator.createEntitySpecV2(
            ENTITY_ID, "", ValueProto.ValueType.Enum.STRING, Collections.emptyMap()));
    coreClient.simpleApplyFeatureTable(
        PROJECT_NAME,
        DataGenerator.createFeatureTableSpec(
            FEATURE_TABLE_NAME,
            ImmutableList.of(ENTITY_ID),
            ImmutableMap.of(FEATURE_NAME, ValueProto.ValueType.Enum.STRING),
            0,
            Collections.emptyMap()));
  }

  @Test
  public void shouldNotAllowUnauthenticatedGetOnlineFeatures() {
    ServingServiceBlockingStub servingStub =
        AuthTestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        AuthTestUtils.createOnlineFeatureRequest(
            PROJECT_NAME, FEATURE_TABLE_NAME, FEATURE_NAME, ENTITY_ID, 1);
    Exception exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              servingStub.getOnlineFeaturesV2(onlineFeatureRequest);
            });

    String expectedMessage = "UNAUTHENTICATED: Authentication failed";
    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedMessage);
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }

  @Test
  void canGetOnlineFeaturesIfAdmin() {
    ServingServiceBlockingStub servingStub =
        AuthTestUtils.getServingServiceStub(true, FEAST_SERVING_PORT, adminCredentials);
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

  @Test
  void canGetOnlineFeaturesIfProjectMember() {
    Map<String, String> memberCredsOptions = new HashMap<>();
    memberCredsOptions.putAll(adminCredentials);
    memberCredsOptions.put(CLIENT_ID, PROJECT_MEMBER_CLIENT_ID);
    ServingServiceBlockingStub servingStub =
        AuthTestUtils.getServingServiceStub(true, FEAST_SERVING_PORT, memberCredsOptions);
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

  @Test
  void cantGetOnlineFeaturesIfNotProjectMember() {
    Map<String, String> notMemberCredsOptions = new HashMap<>();
    notMemberCredsOptions.putAll(adminCredentials);
    notMemberCredsOptions.put(CLIENT_ID, NOT_PROJECT_MEMBER_CLIENT_ID);
    ServingServiceBlockingStub servingStub =
        AuthTestUtils.getServingServiceStub(true, FEAST_SERVING_PORT, notMemberCredsOptions);
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        AuthTestUtils.createOnlineFeatureRequest(
            PROJECT_NAME, FEATURE_TABLE_NAME, FEATURE_NAME, ENTITY_ID, 1);
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> servingStub.getOnlineFeaturesV2(onlineFeatureRequest));

    String expectedMessage =
        String.format(
            "PERMISSION_DENIED: Access denied to project %s for subject %s",
            PROJECT_NAME, NOT_PROJECT_MEMBER_CLIENT_ID);
    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedMessage);
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }
}
