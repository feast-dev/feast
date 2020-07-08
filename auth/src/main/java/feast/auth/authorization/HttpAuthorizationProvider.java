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
package feast.auth.authorization;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import feast.auth.generated.client.api.DefaultApi;
import feast.auth.generated.client.invoker.ApiClient;
import feast.auth.generated.client.invoker.ApiException;
import feast.auth.generated.client.model.CheckProjectAccessRequest;
import feast.auth.generated.client.model.CheckProjectAccessResponse;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

/**
 * HTTPAuthorizationProvider uses an external HTTP service for authorizing requests. Please see
 * auth/src/main/resources/api.yaml for the API specification of this external service.
 */
public class HttpAuthorizationProvider implements AuthorizationProvider {

  private static final Logger log = LoggerFactory.getLogger(HttpAuthorizationProvider.class);
  private final DefaultApi defaultApiClient;

  /**
   * Initializes the HTTPAuthorizationProvider
   *
   * @param options String K/V pair of options to initialize the provider with. Expects at least a
   *     "basePath" for the provider URL
   */
  public HttpAuthorizationProvider(Map<String, String> options) {
    if (options == null) {
      throw new IllegalArgumentException(
          "Cannot pass empty or null options to HTTPAuthorizationProvider");
    }

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(options.get("externalAuthUrl"));
    this.defaultApiClient = new DefaultApi(apiClient);
  }

  /**
   * Validates whether a user has access to the project
   *
   * @param project Name of the Feast project
   * @param authentication Spring Security Authentication object
   * @return AuthorizationResult result of authorization query
   */
  public AuthorizationResult checkAccess(String project, Authentication authentication) {
    CheckProjectAccessRequest checkProjectAccessRequest =
        new CheckProjectAccessRequest().project(project).authentication(authentication);

    try {
      // Make authorization request to external service
      CheckProjectAccessResponse response =
          defaultApiClient.checkProjectAccessPost(checkProjectAccessRequest);
      if (response == null || response.getAllowed() == null) {
        throw new RuntimeException(
            String.format(
                "Empty response returned for access to project %s for authentication \n%s",
                project, authenticationToJson(authentication)));
      }
      if (response.getAllowed()) {
        // Successfully authenticated
        return AuthorizationResult.success();
      }
      // Could not determine project membership, deny access.
      return AuthorizationResult.failed(
          String.format(
              "Access denied to project %s for with message: %s", project, response.getMessage()));
    } catch (ApiException e) {
      log.error("API exception has occurred while authenticating user: {}", e.getMessage(), e);
    }

    // Could not determine project membership, deny access.
    return AuthorizationResult.failed(String.format("Access denied to project %s", project));
  }

  /**
   * Converts Spring Authentication object into Json String form.
   *
   * @param authentication Authentication object that contains request level authentication metadata
   * @return Json representation of authentication object
   */
  private static String authenticationToJson(Authentication authentication) {
    ObjectWriter ow =
        new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .writer()
            .withDefaultPrettyPrinter();
    try {
      return ow.writeValueAsString(authentication);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Could not convert Authentication object to JSON: %s", authentication.toString()));
    }
  }
}
