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
package feast.serving.config;

import feast.common.auth.credentials.GoogleAuthCredentials;
import feast.common.auth.credentials.OAuthCredentials;
import feast.proto.serving.ServingServiceGrpc;
import io.grpc.CallCredentials;
import io.grpc.health.v1.HealthGrpc;
import java.io.IOException;
import net.devh.boot.grpc.server.security.check.AccessPredicate;
import net.devh.boot.grpc.server.security.check.GrpcSecurityMetadataSource;
import net.devh.boot.grpc.server.security.check.ManualGrpcSecurityMetadataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@Configuration
@ComponentScan(
    basePackages = {
      "feast.common.auth.config",
      "feast.common.auth.service",
      "feast.common.logging.interceptors"
    })
public class ServingSecurityConfig {

  private final FeastProperties feastProperties;

  public ServingSecurityConfig(FeastProperties feastProperties) {
    this.feastProperties = feastProperties;
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
    source.set(ServingServiceGrpc.getGetFeastServingInfoMethod(), AccessPredicate.permitAll());
    source.set(HealthGrpc.getCheckMethod(), AccessPredicate.permitAll());
    return source;
  }

  /**
   * Creates a CallCredentials when authentication is enabled on core. This allows serving to
   * connect to core with CallCredentials
   *
   * @return CallCredentials
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.core-authentication", name = "enabled")
  CallCredentials CoreGrpcAuthenticationCredentials() throws IOException {
    switch (feastProperties.getCoreAuthentication().getProvider()) {
      case "google":
        return new GoogleAuthCredentials(feastProperties.getCoreAuthentication().getOptions());
      case "oauth":
        return new OAuthCredentials(feastProperties.getCoreAuthentication().getOptions());
      default:
        throw new IllegalArgumentException(
            "Please configure an Core Authentication Provider "
                + "if you have enabled Authentication on core. "
                + "Currently `google` and `oauth` are supported");
    }
  }
}
