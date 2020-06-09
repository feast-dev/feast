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
package feast.core.config;

import feast.core.CoreServiceGrpc;
import feast.core.auth.authentication.GoogleOID.GoogleOpenIDAuthenticationProvider;
import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.auth.authorization.Keto.KetoAuthorizationProvider;
import feast.core.config.FeastProperties.SecurityProperties;
import java.util.ArrayList;
import java.util.List;
import net.devh.boot.grpc.server.security.authentication.BearerAuthenticationReader;
import net.devh.boot.grpc.server.security.authentication.GrpcAuthenticationReader;
import net.devh.boot.grpc.server.security.check.AccessPredicate;
import net.devh.boot.grpc.server.security.check.AccessPredicateVoter;
import net.devh.boot.grpc.server.security.check.GrpcSecurityMetadataSource;
import net.devh.boot.grpc.server.security.check.ManualGrpcSecurityMetadataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.vote.UnanimousBased;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.oauth2.server.resource.BearerTokenAuthenticationToken;

@Configuration
public class SecurityConfig {

  private final SecurityProperties securityProperties;

  public SecurityConfig(FeastProperties feastProperties) {
    this.securityProperties = feastProperties.getSecurity();
  }

  /**
   * Initializes an AuthenticationManager if authentication has been enabled.
   *
   * @return AuthenticationManager
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authentication", name = "enabled")
  AuthenticationManager authenticationManager() {
    final List<AuthenticationProvider> providers = new ArrayList<>();

    if (securityProperties.getAuthentication().isEnabled()) {
      switch (securityProperties.getAuthentication().getProvider()) {
        case "GoogleOpenID":
          providers.add(
              new GoogleOpenIDAuthenticationProvider(
                  securityProperties.getAuthentication().getOptions()));
          break;
        default:
          throw new IllegalArgumentException(
              "Please configure an Authentication Provider if you have enabled authentication.");
      }
    }
    return new ProviderManager(providers);
  }

  /**
   * Creates an AuthenticationReader that the AuthenticationManager will use to authenticate
   * requests
   *
   * @return GrpcAuthenticationReader
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authentication", name = "enabled")
  GrpcAuthenticationReader authenticationReader() {
    return new BearerAuthenticationReader(BearerTokenAuthenticationToken::new);
  }

  /**
   * Creates a SecurityMetadataSource when authentication is enabled. This allows for the
   * configuration of endpoint level security rules.
   *
   * @return GrpcSecurityMetadataSource
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authentication", name = "enabled")
  GrpcSecurityMetadataSource grpcSecurityMetadataSource() {
    final ManualGrpcSecurityMetadataSource source = new ManualGrpcSecurityMetadataSource();

    // Authentication is enabled for all gRPC endpoints
    source.setDefault(AccessPredicate.authenticated());

    // The following endpoints allow unauthenticated access
    source.set(CoreServiceGrpc.getGetFeastCoreVersionMethod(), AccessPredicate.permitAll());

    return source;
  }

  /**
   * Creates an AccessDecisionManager if authorization is enabled. This object determines the policy
   * used to make authorization decisions.
   *
   * @return AccessDecisionManager
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authorization", name = "enabled")
  AccessDecisionManager accessDecisionManager() {
    final List<AccessDecisionVoter<?>> voters = new ArrayList<>();
    voters.add(new AccessPredicateVoter());
    return new UnanimousBased(voters);
  }

  /**
   * Creates an AuthorizationProvider based on Feast configuration. This provider is available
   * through the security service.
   *
   * @return AuthorizationProvider used to validate access to Feast resources.
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authorization", name = "enabled")
  AuthorizationProvider authorizationProvider() {
    if (securityProperties.getAuthentication().isEnabled()
        && securityProperties.getAuthorization().isEnabled()) {
      switch (securityProperties.getAuthorization().getProvider()) {
        case "KetoAuthorization":
          return new KetoAuthorizationProvider(securityProperties.getAuthorization().getOptions());
        default:
          throw new IllegalArgumentException(
              "Please configure an Authorization Provider if you have enabled authorization.");
      }
    }
    return null;
  }
}
