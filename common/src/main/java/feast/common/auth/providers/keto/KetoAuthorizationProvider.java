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
import feast.common.auth.utils.AuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicyAllowedInput;

public class KetoAuthorizationProvider implements AuthorizationProvider {

  /** Builder for KetoAuthorizationProvider */
  public static class Builder {
    private final String url;
    private String subjectClaim = "email";
    private String flavor = "glob";
    private String action = "edit";
    private String subjectPrefix = "";
    private String resourcePrefix = "";

    /**
     * Initialized builder for Keto authorization provider.
     *
     * @param url Url string for Keto server.
     * @return Returns Builder
     */
    public Builder(String url) {
      this.url = url;
    }

    /**
     * Set subject claim for authentication
     *
     * @param subjectClaim Subject claim. Default: email.
     * @return Returns Builder
     */
    public Builder withSubjectClaim(String subjectClaim) {
      this.subjectClaim = subjectClaim;
      return this;
    }

    /**
     * Set flavor for Keto authorization. One of [exact, glob regex]
     *
     * @param flavor Keto authorization flavor. Default: glob.
     * @return Returns Builder
     */
    public Builder withFlavor(String flavor) {
      this.flavor = flavor;
      return this;
    }

    /**
     * Set action that corresponds to the permission to edit a Feast project resource.
     *
     * @param action Keto action. Default: edit.
     * @return Returns Builder
     */
    public Builder withAction(String action) {
      this.action = action;
      return this;
    }

    /**
     * If set, The subject will be prefixed before sending the request to Keto. Example:
     * users:someuser@email.com
     *
     * @param prefix Subject prefix. Default: Empty string.
     * @return Returns Builder
     */
    public Builder withSubjectPrefix(String prefix) {
      this.subjectPrefix = prefix;
      return this;
    }

    /**
     * If set, The resource will be prefixed before sending the request to Keto. Example:
     * projects:somefeastproject
     *
     * @param prefix Resource prefix. Default: Empty string.
     * @return Returns Builder
     */
    public Builder withResourcePrefix(String prefix) {
      this.resourcePrefix = prefix;
      return this;
    }

    /**
     * Build KetoAuthorizationProvider
     *
     * @return Returns KetoAuthorizationProvider
     */
    public KetoAuthorizationProvider build() {
      return new KetoAuthorizationProvider(this);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(KetoAuthorizationProvider.class);

  private final EnginesApi apiInstance;
  private final String subjectClaim;
  private final String flavor;
  private final String action;
  private final String subjectPrefix;
  private final String resourcePrefix;

  private KetoAuthorizationProvider(Builder builder) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath(builder.url);
    apiInstance = new EnginesApi(defaultClient);
    subjectClaim = builder.subjectClaim;
    flavor = builder.flavor;
    action = builder.action;
    subjectPrefix = builder.subjectPrefix;
    resourcePrefix = builder.resourcePrefix;
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
