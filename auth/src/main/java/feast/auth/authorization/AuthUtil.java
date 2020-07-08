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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.Map;
import org.hibernate.validator.internal.constraintvalidators.bv.EmailValidator;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;

public class AuthUtil {

  /**
   * Get user email from their authentication object.
   *
   * @param authentication Spring Security Authentication object, used to extract user details
   * @return String user email
   */
  public static String getEmailFromAuth(Authentication authentication) {
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

  /**
   * Converts Spring Authentication object into Json String form.
   *
   * @param authentication Authentication object that contains request level authentication metadata
   * @return Json representation of authentication object
   */
  public static String authenticationToJson(Authentication authentication) {
    ObjectWriter ow =
        new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .writer()
            .withDefaultPrettyPrinter();
    try {
      return ow.writeValueAsString(authentication);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Could not convert Authentication object to JSON: %s", authentication.toString()));
    }
  }
}
