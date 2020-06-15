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
package feast.core.auth.authorization.Keto;

import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.auth.authorization.AuthorizationResult;
import java.util.List;
import java.util.Map;
import org.hibernate.validator.internal.constraintvalidators.bv.EmailValidator;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicyRole;

/** Authorization Provider implementation for Ory Keto */
public class KetoAuthorizationProvider implements AuthorizationProvider {

  private final EnginesApi apiInstance;

  /**
   * Initializes the KetoAuthorizationProvider
   *
   * @param options String K/V pair of options to initialize the provider with. Expects at least a
   *     "basePath" for the provider URL
   */
  public KetoAuthorizationProvider(Map<String, String> options) {
    if (options == null) {
      throw new IllegalArgumentException("Cannot pass empty or null options to KetoAuth");
    }
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath(options.get("basePath"));
    this.apiInstance = new EnginesApi(defaultClient);
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
    try {
      // Get all roles from Keto
      List<OryAccessControlPolicyRole> roles =
          this.apiInstance.listOryAccessControlPolicyRoles("glob", 500L, 500L, email);

      // Loop through all roles the user has
      for (OryAccessControlPolicyRole role : roles) {
        // If the user has an admin or project specific role, return.
        if (("roles:admin").equals(role.getId())
            || (String.format("roles:feast:%s-member", project)).equals(role.getId())) {
          return AuthorizationResult.success();
        }
      }
    } catch (ApiException e) {
      System.err.println("Exception when calling EnginesApi#doOryAccessControlPoliciesAllow");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
    // Could not determine project membership, deny access.
    return AuthorizationResult.failed(
        String.format("Access denied to project %s for user %s", project, email));
  }

  /**
   * Get user email from their authentication object.
   *
   * @param authentication Spring Security Authentication object, used to extract user details
   * @return String user email
   */
  private String getEmailFromAuth(Authentication authentication) {
    Jwt principle = ((Jwt) authentication.getPrincipal());
    Map<String, Object> claims = principle.getClaims();
    String email = (String) claims.get("email");

    if (email.isEmpty()) {
      throw new IllegalStateException("JWT does not have a valid email set.");
    }
    boolean validEmail = (new EmailValidator()).isValid(email, null);
    if (!validEmail) {
      throw new IllegalStateException("JWT contains an invalid email address");
    }
    return email;
  }
}
