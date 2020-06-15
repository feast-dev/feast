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
package feast.core.auth.authorization;

import org.springframework.security.core.Authentication;

/**
 * AuthorizationProvider is the base interface that each AuthorizationProvider needs to implement in
 * order to authorize requests to Feast Core
 */
public interface AuthorizationProvider {

  /**
   * Validates whether a user is allowed access to the project
   *
   * @param project Name of the Feast project
   * @param authentication Spring Security Authentication object
   * @return AuthorizationResult result of authorization query
   */
  AuthorizationResult checkAccess(String project, Authentication authentication);
}
