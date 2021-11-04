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
package feast.common.auth.service;

import feast.common.auth.authorization.AuthorizationProvider;
import feast.common.auth.authorization.AuthorizationResult;
import feast.common.auth.config.SecurityProperties;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.stereotype.Service;

@AllArgsConstructor
@Service
public class AuthorizationService {

  private final SecurityProperties securityProperties;
  private final AuthorizationProvider authorizationProvider;

  @Autowired
  public AuthorizationService(
      SecurityProperties securityProperties,
      ObjectProvider<AuthorizationProvider> authorizationProvider) {
    this.securityProperties = securityProperties;
    this.authorizationProvider = authorizationProvider.getIfAvailable();
  }

  /**
   * Determine whether a user has access to a project.
   *
   * @param securityContext Spring Security Context used to identify a user or service.
   * @param project Name of the project for which membership should be tested.
   */
  public void authorizeRequest(SecurityContext securityContext, String project) {
    Authentication authentication = securityContext.getAuthentication();
    if (!this.securityProperties.getAuthorization().isEnabled()) {
      return;
    }

    AuthorizationResult result =
        this.authorizationProvider.checkAccessToProject(project, authentication);
    if (!result.isAllowed()) {
      throw new AccessDeniedException(result.getFailureReason().orElse("Access Denied"));
    }
  }
}
