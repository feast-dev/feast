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
package feast.common.auth.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import feast.common.auth.config.CacheConfiguration;
import feast.common.auth.config.SecurityProperties;
import feast.common.auth.config.SecurityProperties.AuthenticationProperties;
import feast.common.auth.config.SecurityProperties.AuthorizationProperties;
import feast.common.auth.providers.http.HttpAuthorizationProvider;
import feast.common.auth.providers.http.client.api.DefaultApi;
import feast.common.auth.providers.http.client.model.AuthorizationResult;
import feast.common.auth.providers.http.client.model.CheckAccessRequest;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(
    classes = {CacheConfiguration.class, HttpAuthorizationProviderCachingTest.Config.class})
public class HttpAuthorizationProviderCachingTest {

  // static since field needs to updated in provider() bean
  private static DefaultApi api = Mockito.mock(DefaultApi.class);

  @Autowired AuthorizationProvider provider;

  @Configuration
  static class Config {
    @Bean
    SecurityProperties securityProps() {
      // setting TTL static variable in SecurityProperties bean, since CacheConfiguration bean is
      // dependent on SecurityProperties.
      CacheConfiguration.TTL = 1;
      AuthenticationProperties authentication = Mockito.mock(AuthenticationProperties.class);
      AuthorizationProperties authorization = new AuthorizationProperties();
      authorization.setEnabled(true);
      authorization.setProvider("http");
      authorization.setOptions(ImmutableMap.of("authorizationUrl", "localhost"));

      authentication.setOptions(ImmutableMap.of("subjectClaim", "email"));

      SecurityProperties sp = new SecurityProperties();
      sp.setAuthentication(authentication);
      sp.setAuthorization(authorization);
      return sp;
    }

    @Bean
    AuthorizationProvider provider() throws NoSuchFieldException, SecurityException {
      Map<String, String> options = new HashMap<>();
      options.put("authorizationUrl", "localhost");
      options.put("subjectClaim", "email");
      HttpAuthorizationProvider provider = new HttpAuthorizationProvider(options);
      FieldSetter.setField(provider, provider.getClass().getDeclaredField("defaultApiClient"), api);
      return provider;
    }
  }

  @Test
  public void testCheckAccessToProjectShouldReadFromCacheWhenAvailable() throws Exception {
    Authentication auth = Mockito.mock(Authentication.class);
    Jwt jwt = Mockito.mock(Jwt.class);
    Map<String, Object> claims = new HashMap<>();
    claims.put("email", "test@test.com");
    doReturn(jwt).when(auth).getCredentials();
    doReturn(jwt).when(auth).getPrincipal();
    doReturn(claims).when(jwt).getClaims();
    doReturn("test_token").when(jwt).getTokenValue();
    AuthorizationResult authResult = new AuthorizationResult();
    authResult.setAllowed(true);
    doReturn(authResult)
        .when(api)
        .checkAccessPost(any(CheckAccessRequest.class), any(String.class));

    // Should save the result in cache
    provider.checkAccessToProject("test", auth);
    // Should read from cache
    provider.checkAccessToProject("test", auth);
    verify(api, times(1)).checkAccessPost(any(CheckAccessRequest.class), any(String.class));

    // cache ttl is set to 1 second for testing.
    Thread.sleep(1100);

    // Should make an invocation to external service
    provider.checkAccessToProject("test", auth);
    verify(api, times(2)).checkAccessPost(any(CheckAccessRequest.class), any(String.class));
    // Should read from cache
    provider.checkAccessToProject("test", auth);
    verify(api, times(2)).checkAccessPost(any(CheckAccessRequest.class), any(String.class));
  }
}
