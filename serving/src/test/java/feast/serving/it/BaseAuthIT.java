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

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@ActiveProfiles("it")
@SpringBootTest
public class BaseAuthIT {

  static final String FEATURE_TABLE_NAME = "featuretable_1";
  static final String FEATURE_NAME = "feature_1";
  static final String ENTITY_ID = "entity_id";
  static final String PROJECT_NAME = "project_1";
  static final int SERVICE_START_MAX_WAIT_TIME_IN_MINUTES = 3;
  static final String CLIENT_ID = "client_id";
  static final String CLIENT_SECRET = "client_secret";
  static final String TOKEN_URL = "http://localhost:4444/oauth2/token";
  static final String JWK_URI = "http://localhost:4444/.well-known/jwks.json";

  static final String GRANT_TYPE = "client_credentials";

  static final String AUDIENCE = "https://localhost";

  static final String CORE = "core_1";

  static final String HYDRA = "hydra_1";
  static final int HYDRA_PORT = 4445;

  static CoreSimpleAPIClient insecureApiClient;

  static final String REDIS = "redis_1";
  static final int REDIS_PORT = 6379;

  static final int FEAST_CORE_PORT = 6565;

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("feast.stores[0].name", () -> "online");
    registry.add("feast.stores[0].type", () -> "REDIS");
    // Redis needs to accessible by both core and serving, hence using host address
    registry.add(
        "feast.stores[0].config.host",
        () -> {
          try {
            return InetAddress.getLocalHost().getHostAddress();
          } catch (UnknownHostException e) {
            e.printStackTrace();
            return "";
          }
        });
    registry.add("feast.stores[0].config.port", () -> REDIS_PORT);
    registry.add("feast.stores[0].subscriptions[0].name", () -> "*");
    registry.add("feast.stores[0].subscriptions[0].project", () -> "*");

    registry.add("feast.core-authentication.options.oauth_url", () -> TOKEN_URL);
    registry.add("feast.core-authentication.options.grant_type", () -> GRANT_TYPE);
    registry.add("feast.core-authentication.options.client_id", () -> CLIENT_ID);
    registry.add("feast.core-authentication.options.client_secret", () -> CLIENT_SECRET);
    registry.add("feast.core-authentication.options.audience", () -> AUDIENCE);
    registry.add("feast.core-authentication.options.jwkEndpointURI", () -> JWK_URI);
    registry.add("feast.security.authentication.options.jwkEndpointURI", () -> JWK_URI);
  }
}
