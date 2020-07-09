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
package feast.auth.authorization.Keto;

import feast.auth.authorization.AuthorizationProvider;
import feast.auth.authorization.AuthorizationResult;
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
  private final Map<String, Object> options;

  /**
   * Initializes the KetoAuthorizationProvider
   *
   * @param options String K/V pair of options to initialize the provider with. Expects at least a
   *     "basePath" for the provider URL
   */
  public KetoAuthorizationProvider(Map<String, Object> options) {
    if (options == null) {
      throw new IllegalArgumentException("Cannot pass empty or null options to KetoAuth");
    }
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath((String)options.get("basePath"));
    this.apiInstance = new EnginesApi(defaultClient);
    this.options = options;
  }

  /**
   * Validates whether a user has access to the project
   *
   * @param project Name of the Feast project
   * @param authentication Spring Security Authentication object
   * @return AuthorizationResult result of authorization query
   */
  public AuthorizationResult checkAccess(String project, Authentication authentication) {
    String subject = (String)this.options.get("subject");
    if ((subject == null) || (subject.isEmpty()))
      subject = "email";
    String subjectValue = getSubjectFromAuth(authentication, subject);
    try {
      // Get all roles from Keto
      List<OryAccessControlPolicyRole> roles =
          this.apiInstance.listOryAccessControlPolicyRoles("glob", 500L, 500L, subjectValue);
      List<String> roleTemplates = (List<String>) this.options.get("roles");

      for (String roleTemplate : roleTemplates) {
        if (roleTemplate.contains("{PROJECT}")) {
          roleTemplate = roleTemplate.replace("{PROJECT}", project);
        }
        // Loop through all roles the user has
        for (OryAccessControlPolicyRole role : roles) {
          // If the user has a role matching one of the templates, return.
          if ((roleTemplate).equals(role.getId())) {
            return AuthorizationResult.success();
          }
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
   * @param subject Subject from the Auth token
   * @return String user email
   */
  private String getSubjectFromAuth(Authentication authentication, String subject) {
    Jwt principle = ((Jwt) authentication.getPrincipal());
    Map<String, Object> claims = principle.getClaims();
    String subjectValue = (String)claims.get(subject);

    if (subjectValue.isEmpty()) {
      throw new IllegalStateException(String.format("JWT does not have a valid %s.", subject));
    }

    if (subject.equals("email")) {
      boolean validEmail = (new EmailValidator()).isValid(subjectValue, null);
      if (!validEmail) {
        throw new IllegalStateException("JWT contains an invalid email address");
      }
    }

    return subjectValue;
  }
}
