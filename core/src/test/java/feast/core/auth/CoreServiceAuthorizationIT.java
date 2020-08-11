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
package feast.core.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import feast.core.auth.infra.JwtHelper;
import feast.core.config.FeastProperties;
import feast.core.it.BaseIT;
import feast.core.it.DataGenerator;
import feast.core.it.SimpleAPIClient;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.FeatureSetProto;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicy;
import sh.ory.keto.model.OryAccessControlPolicyRole;

@SpringBootTest(
    properties = {
      "feast.security.authentication.enabled=true",
      "feast.security.authorization.enabled=true",
      "feast.security.authorization.provider=http",
    })
public class CoreServiceAuthorizationIT extends BaseIT {

  @Autowired FeastProperties feastProperties;

  private static final String DEFAULT_FLAVOR = "glob";
  private static int KETO_PORT = 4466;
  private static int KETO_ADAPTOR_PORT = 8080;
  private static int feast_core_port;
  private static int JWKS_PORT = 45124;

  private static JwtHelper jwtHelper = new JwtHelper();

  static String project = "myproject";
  static String subjectInProject = "good_member@example.com";
  static String subjectIsAdmin = "bossman@example.com";
  static String subjectClaim = "sub";

  static SimpleAPIClient insecureApiClient;

  @ClassRule public static WireMockClassRule wireMockRule = new WireMockClassRule(JWKS_PORT);

  @Rule public WireMockClassRule instanceRule = wireMockRule;

  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(new File("src/test/resources/keto/docker-compose.yml"))
          .withExposedService("adaptor_1", KETO_ADAPTOR_PORT)
          .withExposedService("keto_1", KETO_PORT, forHttp("/health/ready").forStatusCode(200));

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {

    // Start Keto and with Docker Compose
    environment.start();

    // Seed Keto with data
    String ketoExternalHost = environment.getServiceHost("keto_1", KETO_PORT);
    Integer ketoExternalPort = environment.getServicePort("keto_1", KETO_PORT);
    String ketoExternalUrl = String.format("http://%s:%s", ketoExternalHost, ketoExternalPort);
    try {
      seedKeto(ketoExternalUrl);
    } catch (ApiException e) {
      throw new RuntimeException(String.format("Could not seed Keto store %s", ketoExternalUrl));
    }

    // Start Wiremock Server to act as fake JWKS server
    wireMockRule.start();
    JWKSet keySet = jwtHelper.getKeySet();
    String jwksJson = String.valueOf(keySet.toPublicJWKSet().toJSONObject());

    // When Feast Core looks up a Json Web Token Key Set, we provide our self-signed public key
    wireMockRule.stubFor(
        WireMock.get(WireMock.urlPathEqualTo("/.well-known/jwks.json"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(jwksJson)));

    String jwkEndpointURI =
        String.format("http://localhost:%s/.well-known/jwks.json", wireMockRule.port());

    // Get Keto Authorization Server (Adaptor) url
    String ketoAdaptorHost = environment.getServiceHost("adaptor_1", KETO_ADAPTOR_PORT);
    Integer ketoAdaptorPort = environment.getServicePort("adaptor_1", KETO_ADAPTOR_PORT);
    String ketoAdaptorUrl = String.format("http://%s:%s", ketoAdaptorHost, ketoAdaptorPort);

    // Initialize dynamic properties
    registry.add("feast.security.authorization.options.subjectClaim", () -> subjectClaim);
    registry.add("feast.security.authentication.options.jwkEndpointURI", () -> jwkEndpointURI);
    registry.add("feast.security.authorization.options.authorizationUrl", () -> ketoAdaptorUrl);
  }

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    feast_core_port = port;
    // Create insecure Feast Core gRPC client
    Channel insecureChannel =
        ManagedChannelBuilder.forAddress("localhost", feast_core_port).usePlaintext().build();
    CoreServiceGrpc.CoreServiceBlockingStub insecureCoreService =
        CoreServiceGrpc.newBlockingStub(insecureChannel);
    insecureApiClient = new SimpleAPIClient(insecureCoreService);
  }

  @BeforeEach
  public void setUp() {
    SimpleAPIClient secureApiClient = getSecureApiClient(subjectIsAdmin);
    FeatureSetProto.FeatureSet expectedFeatureSet = DataGenerator.getDefaultFeatureSet();
    secureApiClient.simpleApplyFeatureSet(expectedFeatureSet);
  }

  @AfterAll
  static void tearDown() {
    environment.stop();
    wireMockRule.stop();
  }

  @Test
  public void shouldGetVersionFromFeastCoreAlways() {
    SimpleAPIClient secureApiClient = getSecureApiClient("fakeUserThatIsAuthenticated@example.com");

    String feastCoreVersionSecure = secureApiClient.getFeastCoreVersion();
    String feastCoreVersionInsecure = insecureApiClient.getFeastCoreVersion();

    assertEquals(feastCoreVersionSecure, feastCoreVersionInsecure);
    assertEquals(feastProperties.getVersion(), feastCoreVersionSecure);
  }

  @Test
  public void shouldNotAllowUnauthenticatedFeatureSetListing() {
    Exception exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              insecureApiClient.simpleListFeatureSets("8");
            });

    String expectedMessage = "UNAUTHENTICATED: Authentication failed";
    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedMessage);
  }

  @Test
  public void shouldAllowAuthenticatedFeatureSetListing() {
    SimpleAPIClient secureApiClient =
        getSecureApiClient("AuthenticatedUserWithoutAuthorization@example.com");
    FeatureSetProto.FeatureSet expectedFeatureSet = DataGenerator.getDefaultFeatureSet();
    List<FeatureSetProto.FeatureSet> listFeatureSetsResponse =
        secureApiClient.simpleListFeatureSets("*");
    FeatureSetProto.FeatureSet actualFeatureSet = listFeatureSetsResponse.get(0);

    assert listFeatureSetsResponse.size() == 1;
    assertEquals(
        actualFeatureSet.getSpec().getProject(), expectedFeatureSet.getSpec().getProject());
    assertEquals(
        actualFeatureSet.getSpec().getProject(), expectedFeatureSet.getSpec().getProject());
  }

  @Test
  void cantApplyFeatureSetIfNotProjectMember() throws InvalidProtocolBufferException {
    String userName = "random_user@example.com";
    SimpleAPIClient secureApiClient = getSecureApiClient(userName);
    FeatureSetProto.FeatureSet expectedFeatureSet =
        DataGenerator.createFeatureSet(DataGenerator.getDefaultSource(), project, "test_5");

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> secureApiClient.simpleApplyFeatureSet(expectedFeatureSet));

    String expectedMessage =
        String.format(
            "PERMISSION_DENIED: Access denied to project %s for subject %s", project, userName);
    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedMessage);
  }

  @Test
  void canApplyFeatureSetIfProjectMember() {
    SimpleAPIClient secureApiClient = getSecureApiClient(subjectInProject);
    FeatureSetProto.FeatureSet expectedFeatureSet =
        DataGenerator.createFeatureSet(DataGenerator.getDefaultSource(), project, "test_6");

    secureApiClient.simpleApplyFeatureSet(expectedFeatureSet);

    FeatureSetProto.FeatureSet actualFeatureSet =
        secureApiClient.simpleGetFeatureSet(project, "test_6");

    assertEquals(
        expectedFeatureSet.getSpec().getProject(), actualFeatureSet.getSpec().getProject());
    assertEquals(expectedFeatureSet.getSpec().getName(), actualFeatureSet.getSpec().getName());
    assertEquals(expectedFeatureSet.getSpec().getSource(), actualFeatureSet.getSpec().getSource());
  }

  @Test
  void canApplyFeatureSetIfAdmin() {
    SimpleAPIClient secureApiClient = getSecureApiClient(subjectIsAdmin);
    FeatureSetProto.FeatureSet expectedFeatureSet =
        DataGenerator.createFeatureSet(DataGenerator.getDefaultSource(), "any_project", "test_2");

    secureApiClient.simpleApplyFeatureSet(expectedFeatureSet);

    FeatureSetProto.FeatureSet actualFeatureSet =
        secureApiClient.simpleGetFeatureSet("any_project", "test_2");

    assertEquals(
        expectedFeatureSet.getSpec().getProject(), actualFeatureSet.getSpec().getProject());
    assertEquals(expectedFeatureSet.getSpec().getName(), actualFeatureSet.getSpec().getName());
    assertEquals(expectedFeatureSet.getSpec().getSource(), actualFeatureSet.getSpec().getSource());
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}

  private static void seedKeto(String url) throws ApiException {
    ApiClient ketoClient = Configuration.getDefaultApiClient();
    ketoClient.setBasePath(url);
    EnginesApi enginesApi = new EnginesApi(ketoClient);

    // Add policies
    OryAccessControlPolicy adminPolicy = getAdminPolicy();
    enginesApi.upsertOryAccessControlPolicy(DEFAULT_FLAVOR, adminPolicy);

    OryAccessControlPolicy projectPolicy = getMyProjectMemberPolicy();
    enginesApi.upsertOryAccessControlPolicy(DEFAULT_FLAVOR, projectPolicy);

    // Add policy roles
    OryAccessControlPolicyRole adminPolicyRole = getAdminPolicyRole();
    enginesApi.upsertOryAccessControlPolicyRole(DEFAULT_FLAVOR, adminPolicyRole);

    OryAccessControlPolicyRole myProjectMemberPolicyRole = getMyProjectMemberPolicyRole();
    enginesApi.upsertOryAccessControlPolicyRole(DEFAULT_FLAVOR, myProjectMemberPolicyRole);
  }

  private static OryAccessControlPolicyRole getMyProjectMemberPolicyRole() {
    OryAccessControlPolicyRole role = new OryAccessControlPolicyRole();
    role.setId(String.format("roles:%s-project-members", project));
    role.setMembers(Collections.singletonList("users:" + subjectInProject));
    return role;
  }

  private static OryAccessControlPolicyRole getAdminPolicyRole() {
    OryAccessControlPolicyRole role = new OryAccessControlPolicyRole();
    role.setId("roles:admin");
    role.setMembers(Collections.singletonList("users:" + subjectIsAdmin));
    return role;
  }

  private static OryAccessControlPolicy getAdminPolicy() {
    OryAccessControlPolicy policy = new OryAccessControlPolicy();
    policy.setId("policies:admin");
    policy.subjects(Collections.singletonList("roles:admin"));
    policy.resources(Collections.singletonList("resources:**"));
    policy.actions(Collections.singletonList("actions:**"));
    policy.effect("allow");
    policy.conditions(null);
    return policy;
  }

  private static OryAccessControlPolicy getMyProjectMemberPolicy() {
    OryAccessControlPolicy policy = new OryAccessControlPolicy();
    policy.setId(String.format("policies:%s-project-members-policy", project));
    policy.subjects(Collections.singletonList(String.format("roles:%s-project-members", project)));
    policy.resources(
        Arrays.asList(
            String.format("resources:projects:%s", project),
            String.format("resources:projects:%s:**", project)));
    policy.actions(Collections.singletonList("actions:**"));
    policy.effect("allow");
    policy.conditions(null);
    return policy;
  }

  // Create secure Feast Core gRPC client for a specific user
  private static SimpleAPIClient getSecureApiClient(String subjectEmail) {
    CallCredentials callCredentials = null;
    try {
      callCredentials = jwtHelper.getCallCredentials(subjectEmail);
    } catch (JOSEException e) {
      throw new RuntimeException(
          String.format("Could not build call credentials: %s", e.getMessage()));
    }
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feast_core_port).usePlaintext().build();

    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        CoreServiceGrpc.newBlockingStub(secureChannel).withCallCredentials(callCredentials);

    return new SimpleAPIClient(secureCoreService);
  }
}
