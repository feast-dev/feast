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
package feast.common.auth.utils;

import java.util.Map;
import org.hibernate.validator.internal.constraintvalidators.bv.EmailValidator;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;

public class AuthUtils {
  // Suppresses default constructor, ensuring non-instantiability.
  private AuthUtils() {}

  /**
   * Get user email from their authentication object.
   *
   * @param authentication Spring Security Authentication object, used to extract user details
   * @param subjectClaim Indicates the claim where the subject can be found
   * @return String user email
   */
  public static String getSubjectFromAuth(Authentication authentication, String subjectClaim) {
    Jwt principle = ((Jwt) authentication.getPrincipal());
    Map<String, Object> claims = principle.getClaims();
    String subjectValue = (String) claims.getOrDefault(subjectClaim, "");

    if (subjectValue.isEmpty()) {
      throw new IllegalStateException(
          String.format("JWT does not have a valid claim %s.", subjectClaim));
    }

    if (subjectClaim.equals("email")) {
      boolean validEmail = (new EmailValidator()).isValid(subjectValue, null);
      if (!validEmail) {
        throw new IllegalStateException("JWT contains an invalid email address");
      }
    }
    return subjectValue;
  }
}
