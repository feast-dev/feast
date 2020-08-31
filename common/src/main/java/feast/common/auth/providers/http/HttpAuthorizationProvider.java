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
package feast.common.auth.providers.http;

import feast.common.auth.authorization.AuthorizationProvider;
import feast.common.auth.authorization.AuthorizationResult;
import feast.common.auth.config.CacheConfiguration;
import feast.common.auth.config.SecurityProperties.AuthenticationProperties;
import feast.common.auth.providers.http.client.api.DefaultApi;
import feast.common.auth.providers.http.client.invoker.ApiClient;
import feast.common.auth.providers.http.client.invoker.ApiException;
import feast.common.auth.providers.http.client.model.CheckAccessRequest;
import feast.common.auth.utils.AuthUtils;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * HTTPAuthorizationProvider uses an external HTTP service for authorizing requests. Please see
 * auth/src/main/resources/api.yaml for the API specification of this external service.
 */
public class HttpAuthorizationProvider implements AuthorizationProvider {

  private static final Logger log = LoggerFactory.getLogger(HttpAuthorizationProvider.class);

  private final DefaultApi defaultApiClient;

  /**
   * The default subject claim is the key within the Authentication object where the user's identity
   * can be found
   */
  private final String subjectClaim;

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
    apiClient.setBasePath(options.get("authorizationUrl"));
    this.defaultApiClient = new DefaultApi(apiClient);
    subjectClaim = options.get(AuthenticationProperties.SUBJECT_CLAIM);
  }

  /**
   * Validates whether a user has access to a project. @Cacheable is using {@link
   * CacheConfiguration} settings to cache output of the method {@link AuthorizationResult} for a
   * specified duration set in cache settings.
   *
   * @param projectId Name of the Feast project
   * @param authentication Spring Security Authentication object
   * @return AuthorizationResult result of authorization query
   */
  @Cacheable(value = CacheConfiguration.AUTHORIZATION_CACHE, keyGenerator = "authKeyGenerator")
  public AuthorizationResult checkAccessToProject(String projectId, Authentication authentication) {

    CheckAccessRequest checkAccessRequest = new CheckAccessRequest();
    Object context = getContext(authentication);
    String subject = AuthUtils.getSubjectFromAuth(authentication, subjectClaim);
    String resource = "projects:" + projectId;
    checkAccessRequest.setAction("ALL");
    checkAccessRequest.setContext(context);
    checkAccessRequest.setResource(resource);
    checkAccessRequest.setSubject(subject);
    try {
      Jwt credentials = ((Jwt) authentication.getCredentials());
      // Make authorization request to external service
      feast.common.auth.providers.http.client.model.AuthorizationResult authResult =
          this.defaultApiClient.checkAccessPost(
              checkAccessRequest, "Bearer " + credentials.getTokenValue());
      if (authResult == null) {
        throw new RuntimeException(
            String.format(
                "Empty response returned for access to project %s for subject %s",
                projectId, subject));
      }
      if (authResult.getAllowed()) {
        // Successfully authenticated
        return AuthorizationResult.success();
      }
    } catch (ApiException e) {
      log.error("API exception has occurred during authorization: {}", e.getMessage(), e);
    }

    // Could not determine project membership, deny access.
    return AuthorizationResult.failed(
        String.format("Access denied to project %s for subject %s", projectId, subject));
  }

  /**
   * Extract a context object to send as metadata to the authorization service
   *
   * @param authentication Spring Security Authentication object
   * @return Returns a context object that will be serialized and sent as metadata to the
   *     authorization service
   */
  private Object getContext(Authentication authentication) {
    // Not implemented yet, left empty
    return new Object();
  }
}
