/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.common.auth.providers.keto;

import feast.common.auth.authorization.AuthorizationProvider;
import feast.common.auth.authorization.AuthorizationResult;
import feast.common.auth.config.SecurityProperties;
import feast.common.auth.utils.AuthUtils;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicyAllowedInput;

public class KetoAuthorizationProvider implements AuthorizationProvider {

  private static final Logger log = LoggerFactory.getLogger(KetoAuthorizationProvider.class);

  private final EnginesApi apiInstance;
  private final String subjectClaim;
  private final String flavor;
  private final String action;
  private final String subjectPrefix;
  private final String resourcePrefix;

  /**
   * Initializes the KetoAuthorizationProvider
   *
   * @param options String K/V pair of options to initialize the provider with. Supported K/V pairs:
   *     authorizationUrl: Keto url. flavor: ORY Access Control Policy flavor. Default is exact.
   *     resourcePrefix: Keto resource should be in the form of 'resource_prefix:feast project
   *     name'. action: Action that corresponds to project access.
   */
  public KetoAuthorizationProvider(Map<String, String> options) {
    if (options == null) {
      throw new IllegalArgumentException(
          "Cannot pass empty or null options to HTTPAuthorizationProvider");
    }

    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath(options.get("authorizationUrl"));
    apiInstance = new EnginesApi(defaultClient);
    subjectClaim = options.get(SecurityProperties.AuthenticationProperties.SUBJECT_CLAIM);
    flavor = options.getOrDefault("flavor", "exact");
    action = options.get("action");
    subjectPrefix = options.get("subjectPrefix");
    resourcePrefix = options.get("resourcePrefix");
  }

  @Override
  public AuthorizationResult checkAccessToProject(String projectId, Authentication authentication) {
    String subject = AuthUtils.getSubjectFromAuth(authentication, subjectClaim);
    OryAccessControlPolicyAllowedInput body = new OryAccessControlPolicyAllowedInput();
    body.setAction(action);
    body.setSubject(String.format("%s%s", subjectPrefix, subject));
    body.setResource(String.format("%s%s", resourcePrefix, projectId));
    try {
      sh.ory.keto.model.AuthorizationResult authResult =
          apiInstance.doOryAccessControlPoliciesAllow(flavor, body);
      if (authResult == null) {
        throw new RuntimeException(
            String.format(
                "Empty response returned for access to project %s for subject %s",
                projectId, subject));
      }
      if (authResult.getAllowed()) {
        return AuthorizationResult.success();
      }
    } catch (ApiException e) {
      log.error("API exception has occurred during authorization: {}", e.getMessage(), e);
    }

    return AuthorizationResult.failed(
        String.format("Access denied to project %s for subject %s", projectId, subject));
  }
}
