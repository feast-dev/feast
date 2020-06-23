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
package feast.core.auth.authentication;

import java.util.Map;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;

/** Json Web Token Authentication Provider used to validate incoming requests to Feast Core. */
public class DefaultJwtAuthenticationProvider implements AuthenticationProvider {

  private JwtAuthenticationProvider authProvider;

  /**
   * @param options String K/V pair of options to initialize the AuthenticationProvider with. Only
   *     one option is currently configurable, the jwkEndpointURI.
   */
  public DefaultJwtAuthenticationProvider(Map<String, String> options) {
    // Endpoint used to retrieve certificates to validate JWT token
    String jwkEndpointURI = options.get("jwkEndpointURI");

    // Provide a custom endpoint to retrieve certificates
    authProvider =
        new JwtAuthenticationProvider(NimbusJwtDecoder.withJwkSetUri(jwkEndpointURI).build());
    authProvider.setJwtAuthenticationConverter(new JwtAuthenticationConverter());
  }

  /**
   * Authenticate a request based on its Spring Security Authentication object
   *
   * @param authentication Authentication object which contains a JWT to validate
   * @return Returns the same authentication object after authentication
   */
  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {
    return authProvider.authenticate(authentication);
  }

  @Override
  public boolean supports(Class<?> aClass) {
    return authProvider.supports(aClass);
  }
}
