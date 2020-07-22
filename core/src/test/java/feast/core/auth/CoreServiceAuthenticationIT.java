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

import static org.junit.jupiter.api.Assertions.assertSame;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import feast.core.auth.infra.JwtHelper;
import feast.core.it.BaseIT;
import feast.core.model.*;
import feast.proto.core.*;
import feast.proto.types.ValueProto;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.prometheus.client.CollectorRegistry;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(
    properties = {
      "feast.security.authentication.enabled=true",
      "feast.security.authorization.enabled=false",
    })
public class CoreServiceAuthenticationIT extends BaseIT {

  private static CoreServiceGrpc.CoreServiceBlockingStub insecureCoreService;

  private static int feast_core_port;
  private static int JWKS_PORT = 45124;

  private static JwtHelper jwtHelper = new JwtHelper();

  static String project = "myproject";
  static String subject = "random@example.com";
  static String subjectClaim = "sub";

  @ClassRule public static WireMockClassRule wireMockRule = new WireMockClassRule(JWKS_PORT);

  @Rule public WireMockClassRule instanceRule = wireMockRule;

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {

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

    // Initialize dynamic properties
    registry.add("feast.security.authorization.options.subjectClaim", () -> subjectClaim);
    registry.add("feast.security.authentication.options.jwkEndpointURI", () -> jwkEndpointURI);
  }

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    feast_core_port = port;
    // Create insecure Feast Core gRPC client
    Channel insecureChannel =
        ManagedChannelBuilder.forAddress("localhost", feast_core_port).usePlaintext().build();
    insecureCoreService = CoreServiceGrpc.newBlockingStub(insecureChannel);
  }

  @AfterAll
  static void tearDown() {
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

  /**
   * For the time being, if authentication is enabled but authorization is disabled, users can still
   * connect to Feast Core as anonymous users. They are not forced to authenticate.
   */
  @Test
  public void shouldAllowUnauthenticatedFeatureSetListing() {
    insecureCoreService.listFeatureSets(
        CoreServiceProto.ListFeatureSetsRequest.newBuilder()
            .setFilter(
                CoreServiceProto.ListFeatureSetsRequest.Filter.newBuilder()
                    .setProject("*")
                    .setFeatureSetName("*")
                    .build())
            .build());
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
  void canApplyFeatureSetIfAuthenticated() throws InvalidProtocolBufferException {
    CoreServiceGrpc.CoreServiceBlockingStub secureClient = getSecureGrpcClient(subject);

    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", project).toProto();
    CoreServiceProto.ApplyFeatureSetRequest request =
        CoreServiceProto.ApplyFeatureSetRequest.newBuilder()
            .setFeatureSet(incomingFeatureSet)
            .build();

    CoreServiceProto.ApplyFeatureSetResponse response = secureClient.applyFeatureSet(request);
    assertSame(response.getStatus(), CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED);
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}

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
        ManagedChannelBuilder.forAddress("localhost", feast_core_port).usePlaintext().build();

    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        CoreServiceGrpc.newBlockingStub(secureChannel).withCallCredentials(callCredentials);
    return secureCoreService;
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
}
