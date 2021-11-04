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

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import feast.common.it.BaseIT;
import feast.common.it.DataGenerator;
import feast.common.it.SimpleCoreClient;
import feast.core.auth.infra.JwtHelper;
import feast.core.config.FeastProperties;
import feast.proto.core.*;
import feast.proto.types.ValueProto;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.SocketUtils;

@SpringBootTest(
    properties = {
      "feast.security.authentication.enabled=true",
      "feast.security.authorization.enabled=false",
    })
public class CoreServiceAuthenticationIT extends BaseIT {

  @Autowired FeastProperties feastProperties;

  private static int feast_core_port;
  private static int JWKS_PORT = SocketUtils.findAvailableTcpPort();

  private static JwtHelper jwtHelper = new JwtHelper();

  static String subjectClaim = "sub";

  @ClassRule public static WireMockClassRule wireMockRule = new WireMockClassRule(JWKS_PORT);

  @Rule public WireMockClassRule instanceRule = wireMockRule;

  static SimpleCoreClient insecureApiClient;

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
    registry.add("feast.security.authentication.options.subjectClaim", () -> subjectClaim);
    registry.add("feast.security.authentication.options.jwkEndpointURI", () -> jwkEndpointURI);
  }

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    feast_core_port = port;

    // Create insecure Feast Core gRPC client
    Channel insecureChannel =
        ManagedChannelBuilder.forAddress("localhost", feast_core_port).usePlaintext().build();
    CoreServiceGrpc.CoreServiceBlockingStub insecureCoreService =
        CoreServiceGrpc.newBlockingStub(insecureChannel);
    insecureApiClient = new SimpleCoreClient(insecureCoreService);
  }

  @AfterAll
  static void tearDown() {
    wireMockRule.stop();
  }

  @Test
  public void shouldGetVersionFromFeastCoreAlways() {
    SimpleCoreClient secureApiClient =
        getSecureApiClient("fakeUserThatIsAuthenticated@example.com");

    String feastCoreVersionSecure = secureApiClient.getFeastCoreVersion();
    String feastCoreVersionInsecure = insecureApiClient.getFeastCoreVersion();

    assertEquals(feastCoreVersionSecure, feastCoreVersionInsecure);
    assertEquals(feastProperties.getVersion(), feastCoreVersionSecure);
  }

  /**
   * If authentication is enabled but authorization is disabled, users can still connect to Feast
   * Core as anonymous users. They are not forced to authenticate.
   */
  @Test
  public void shouldAllowUnauthenticatedEntityApplyAndListing() {
    String project = "default";
    EntityProto.EntitySpecV2 expectedEntitySpec =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));
    insecureApiClient.simpleApplyEntity(project, expectedEntitySpec);

    List<EntityProto.Entity> listEntitiesResponse = insecureApiClient.simpleListEntities(project);
    EntityProto.Entity actualEntity = listEntitiesResponse.get(0);

    assert listEntitiesResponse.size() == 1;
    assertEquals(actualEntity.getSpec().getName(), expectedEntitySpec.getName());
  }

  @Test
  public void shouldAllowAuthenticatedEntityApplyAndListing() {
    SimpleCoreClient secureApiClient =
        getSecureApiClient("AuthenticatedUserWithoutAuthorization@example.com");
    String project = "default";
    EntityProto.EntitySpecV2 expectedEntitySpec =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));
    secureApiClient.simpleApplyEntity(project, expectedEntitySpec);
    List<EntityProto.Entity> listEntitiesResponse = insecureApiClient.simpleListEntities(project);
    EntityProto.Entity actualEntity = listEntitiesResponse.get(0);

    assert listEntitiesResponse.size() == 1;
    assertEquals(actualEntity.getSpec().getName(), expectedEntitySpec.getName());
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}

  // Create secure Feast Core gRPC client for a specific user
  private static SimpleCoreClient getSecureApiClient(String subjectEmail) {
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

    return new SimpleCoreClient(secureCoreService);
  }
}
