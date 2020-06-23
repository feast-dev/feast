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

import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Implementation of AuthorizationProvider will return AuthorizationResult after validating incoming
 * requests to Feast Core. AuthorizationResult provides methods to check if user is authorized to
 * perform an action or not.
 */
@Getter
@AllArgsConstructor
public class AuthorizationResult {

  /**
   * Method to create AuthorizationResult Object.
   *
   * @param allowed True If user is authorized, False otherwise.
   * @param failureReason Reason for authorization failure, if any
   * @return AuthorizationResult Object.
   */
  public static AuthorizationResult create(
      @Nullable boolean allowed, @Nullable String failureReason) {
    return new AuthorizationResult(allowed, Optional.ofNullable(failureReason));
  }

  /**
   * Method to create failed AuthorizationResult Object.
   *
   * @param failureReason Reason for authorization failure, if any or Null
   * @return AuthorizationResult Object.
   */
  public static AuthorizationResult failed(@Nullable String failureReason) {
    return new AuthorizationResult(false, Optional.ofNullable(failureReason));
  }

  /**
   * Method to create Success AuthorizationResult Object.
   *
   * @return AuthorizationResult Object.
   */
  public static AuthorizationResult success() {
    return new AuthorizationResult(true, Optional.empty());
  }

  private boolean allowed;
  private Optional<String> failureReason;
}
