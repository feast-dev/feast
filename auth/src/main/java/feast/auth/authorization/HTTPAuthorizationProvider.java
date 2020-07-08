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

import static feast.auth.authorization.AuthUtil.getEmailFromAuth;

import feast.auth.generated.client.api.DefaultApi;
import feast.auth.generated.client.invoker.ApiClient;
import feast.auth.generated.client.invoker.ApiException;
import feast.auth.generated.client.model.CheckProjectAccessRequest;
import feast.auth.generated.client.model.CheckProjectAccessResponse;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

/** Authorization Provider implementation for external HTTP authorization server */
public class HTTPAuthorizationProvider implements AuthorizationProvider {

  private static final Logger log = LoggerFactory.getLogger(HTTPAuthorizationProvider.class);
  private final DefaultApi defaultApiClient;

  /**
   * Initializes the HTTPAuthorizationProvider
   *
   * @param options String K/V pair of options to initialize the provider with. Expects at least a
   *     "basePath" for the provider URL
   */
  public HTTPAuthorizationProvider(Map<String, String> options) {
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
    String email = getEmailFromAuth(authentication);
    CheckProjectAccessRequest checkProjectAccessRequest =
        new CheckProjectAccessRequest().project(project).authentication(authentication);

    try {
      // Make authorization request to external service
      CheckProjectAccessResponse response =
          defaultApiClient.checkProjectAccessPost(checkProjectAccessRequest);
      if (response == null || response.getAllowed() == null) {
        throw new RuntimeException(
            String.format(
                "Empty response returned for HTTP authorization, email %s, authentication %s",
                email, authentication.toString()));
      }
      if (response.getAllowed()) {
        // Successfully authenticated
        return AuthorizationResult.success();
      }
      // Could not determine project membership, deny access.
      return AuthorizationResult.failed(
          String.format(
              "Access denied to project %s for user %s with message %s",
              project, email, response.getMessage()));
    } catch (ApiException e) {
      log.error("API exception has occurred while authenticating user: {}", e.getMessage(), e);
    }

    // Could not determine project membership, deny access.
    return AuthorizationResult.failed(
        String.format("Access denied to project %s for user %s", project, email));
  }
}
