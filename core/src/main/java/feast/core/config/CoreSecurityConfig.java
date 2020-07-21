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

import feast.proto.core.CoreServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.security.check.AccessPredicate;
import net.devh.boot.grpc.server.security.check.GrpcSecurityMetadataSource;
import net.devh.boot.grpc.server.security.check.ManualGrpcSecurityMetadataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
@ComponentScan("feast.auth")
public class CoreSecurityConfig {

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
    source.set(CoreServiceGrpc.getUpdateStoreMethod(), AccessPredicate.permitAll());
    return source;
  }
}
