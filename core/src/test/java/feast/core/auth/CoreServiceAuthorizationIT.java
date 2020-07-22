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
import feast.core.CoreApplication;
import feast.core.auth.infra.JwtHelper;
import feast.core.model.Entity;
import feast.core.model.Feature;
import feast.core.model.FeatureSet;
import feast.core.model.Source;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceProto;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.types.ValueProto;
import io.grpc.*;
import io.prometheus.client.CollectorRegistry;
import java.io.File;
import java.sql.Date;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicy;
import sh.ory.keto.model.OryAccessControlPolicyRole;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(classes = CoreApplication.class)
@ContextConfiguration(initializers = {CoreServiceAuthorizationIT.Initializer.class})
@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class CoreServiceAuthorizationIT {

  public CoreServiceAuthorizationIT() {}

  private static CoreServiceGrpc.CoreServiceBlockingStub insecureCoreService;

  private static final String DEFAULT_FLAVOR = "glob";
  private static int KETO_PORT = 4466;
  private static int KETO_ADAPTOR_PORT = 8080;
  private static int FEAST_CORE_PORT = 6565;
  private static int JWKS_PORT = 45124;

  private static JwtHelper jwtHelper = null;

  static String project = "myproject";
  static String subjectInProject = "good_member@example.com";
  static String subjectIsAdmin = "bossman@example.com";
  static String subjectClaim = "sub";

  @Rule static PostgreSQLContainer postgres = new PostgreSQLContainer();

  @Rule public static KafkaContainer kafka = new KafkaContainer();

  @ClassRule public static WireMockClassRule wireMockRule = new WireMockClassRule(JWKS_PORT);

  @Rule public WireMockClassRule instanceRule = wireMockRule;

  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(new File("src/test/resources/keto/docker-compose.yml"))
          .withExposedService("adaptor_1", KETO_ADAPTOR_PORT)
          .withExposedService("keto_1", KETO_PORT, forHttp("/health/ready").forStatusCode(200));

  static class Initializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      // Prevent Spring devtools from restarting application
      System.setProperty("spring.devtools.restart.enabled", "false");

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

      // Start postgres
      postgres.start();

      // Start Kafka
      kafka.start();

      // Set up Jwt Helper to generate keys and sign authentication requests
      jwtHelper = new JwtHelper();

      // Start Wiremock Server to act as fake JWKS server
      wireMockRule.start();
      JWKSet keySet = jwtHelper.getKeySet();
      String jwksJson = String.valueOf(keySet.toPublicJWKSet().toJSONObject());

      // When Feast Core looks up a Json Web Token Key Set, we provide our self-signed public

      wireMockRule.stubFor(
          WireMock.get(WireMock.urlPathEqualTo("/.well-known/jwks.json"))
              .willReturn(
                  WireMock.aResponse()
                      .withStatus(200)
                      .withHeader("Content-Type", "application/json")
                      .withBody(jwksJson)));

      // Get Keto Authorization Server (Adaptor) url
      String ketoAdaptorHost = environment.getServiceHost("adaptor_1", KETO_ADAPTOR_PORT);
      Integer ketoAdaptorPort = environment.getServicePort("adaptor_1", KETO_ADAPTOR_PORT);
      String ketoAdaptorUrl = String.format("http://%s:%s", ketoAdaptorHost, ketoAdaptorPort);

      TestPropertyValues.of(
              "spring.datasource.url=" + postgres.getJdbcUrl(),
              "spring.datasource.username=" + postgres.getUsername(),
              "spring.datasource.password=" + postgres.getPassword(),
              "feast.stream.options.bootstrapServers=" + kafka.getBootstrapServers(),
              "feast.security.authentication.enabled=true",
              "feast.security.authorization.enabled=true",
              "feast.security.authorization.provider=http",
              "feast.security.authorization.options.subjectClaim=" + subjectClaim,
              "feast.security.authorization.options.authorizationUrl=" + ketoAdaptorUrl,
              String.format(
                  "feast.security.authentication.options.jwkEndpointURI=http://localhost:%s/.well-known/jwks.json",
                  wireMockRule.port()))
          .applyTo(configurableApplicationContext.getEnvironment());

      // Create insecure Feast Core gRPC client
      Channel insecureChannel =
          ManagedChannelBuilder.forAddress("localhost", FEAST_CORE_PORT).usePlaintext().build();
      insecureCoreService = CoreServiceGrpc.newBlockingStub(insecureChannel);
    }
  }

  @AfterAll
  static void tearDown() {
    environment.stop();
    postgres.stop();
    kafka.stop();
    wireMockRule.stop();
    CollectorRegistry.defaultRegistry.clear();
  }

  @Test
  public void shouldGetVersionFromFeastCoreAlways() {
    insecureCoreService.getFeastCoreVersion(
        CoreServiceProto.GetFeastCoreVersionRequest.getDefaultInstance());

    CoreServiceGrpc.CoreServiceBlockingStub secureService =
        getSecureGrpcClient("fakeUserThatIsAuthenticated@example.com");
    secureService.getFeastCoreVersion(
        CoreServiceProto.GetFeastCoreVersionRequest.getDefaultInstance());
  }

  @Test
  public void shouldNotAllowUnauthenticatedFeatureSetListing() {
    Exception exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              insecureCoreService.listFeatureSets(
                  CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                      .setFilter(
                          CoreServiceProto.ListFeatureSetsRequest.Filter.newBuilder()
                              .setProject("*")
                              .setFeatureSetName("*")
                              .build())
                      .build());
            });

    String expectedMessage = "UNAUTHENTICATED: Authentication failed";
    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedMessage);
  }

  @Test
  public void shouldAllowAuthenticatedFeatureSetListing() {
    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        getSecureGrpcClient("AuthenticatedUserWithoutAuthorization@example.com");
    secureCoreService.listFeatureSets(
        CoreServiceProto.ListFeatureSetsRequest.newBuilder()
            .setFilter(
                CoreServiceProto.ListFeatureSetsRequest.Filter.newBuilder()
                    .setProject("*")
                    .setFeatureSetName("*")
                    .build())
            .build());
  }

  @Test
  void cantApplyFeatureSetIfNotProjectMember() throws InvalidProtocolBufferException {
    String userName = "random_user@example.com";
    CoreServiceGrpc.CoreServiceBlockingStub secureClient = getSecureGrpcClient(userName);

    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", project).toProto();
    CoreServiceProto.ApplyFeatureSetRequest request =
        CoreServiceProto.ApplyFeatureSetRequest.newBuilder()
            .setFeatureSet(incomingFeatureSet)
            .build();

    StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> secureClient.applyFeatureSet(request));

    String expectedMessage =
        String.format(
            "PERMISSION_DENIED: Access denied to project %s for subject %s", project, userName);
    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedMessage);
  }

  @Test
  void canApplyFeatureSetIfProjectMember() throws InvalidProtocolBufferException {
    CoreServiceGrpc.CoreServiceBlockingStub secureClient = getSecureGrpcClient(subjectInProject);

    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", project).toProto();
    CoreServiceProto.ApplyFeatureSetRequest request =
        CoreServiceProto.ApplyFeatureSetRequest.newBuilder()
            .setFeatureSet(incomingFeatureSet)
            .build();

    feast.proto.core.CoreServiceProto.ApplyFeatureSetResponse response =
        secureClient.applyFeatureSet(request);
    assertSame(response.getStatus(), CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED);
  }

  @Test
  void canApplyFeatureSetIfAdmin() throws InvalidProtocolBufferException {
    CoreServiceGrpc.CoreServiceBlockingStub secureClient = getSecureGrpcClient(subjectIsAdmin);

    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f3", project).toProto();
    CoreServiceProto.ApplyFeatureSetRequest request =
        CoreServiceProto.ApplyFeatureSetRequest.newBuilder()
            .setFeatureSet(incomingFeatureSet)
            .build();

    feast.proto.core.CoreServiceProto.ApplyFeatureSetResponse response =
        secureClient.applyFeatureSet(request);
    assertSame(response.getStatus(), CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED);
  }

  private FeatureSet newDummyFeatureSet(String name, String project) {
    Feature feature = new Feature("feature", ValueProto.ValueType.Enum.INT64);
    Entity entity = new Entity("entity", ValueProto.ValueType.Enum.STRING);
    SourceProto.Source sourceSpec =
        SourceProto.Source.newBuilder()
            .setType(SourceProto.SourceType.KAFKA)
            .setKafkaSourceConfig(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("my-topic")
                    .build())
            .build();
    Source defaultSource = Source.fromProto(sourceSpec);
    FeatureSet fs =
        new FeatureSet(
            name,
            project,
            100L,
            Arrays.asList(entity),
            Arrays.asList(feature),
            defaultSource,
            new HashMap<String, String>(),
            FeatureSetProto.FeatureSetStatus.STATUS_READY);
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }

  // Create secure Feast Core gRPC client for a specific user
  private CoreServiceGrpc.CoreServiceBlockingStub getSecureGrpcClient(String subjectEmail) {
    CallCredentials callCredentials = null;
    try {
      callCredentials = jwtHelper.getCallCredentials(subjectEmail);
    } catch (JOSEException e) {
      throw new RuntimeException(
          String.format("Could not build call credentials: %s", e.getMessage()));
    }
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", FEAST_CORE_PORT).usePlaintext().build();

    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        CoreServiceGrpc.newBlockingStub(secureChannel).withCallCredentials(callCredentials);
    return secureCoreService;
  }

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
}
