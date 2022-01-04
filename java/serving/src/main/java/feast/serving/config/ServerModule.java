/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import feast.serving.grpc.OnlineServingGrpcServiceV2;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentracing.contrib.grpc.TracingServerInterceptor;

public class ServerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(OnlineServingGrpcServiceV2.class);
  }

  @Provides
  public Server provideGrpcServer(
      ApplicationProperties applicationProperties,
      OnlineServingGrpcServiceV2 onlineServingGrpcServiceV2,
      TracingServerInterceptor tracingServerInterceptor) {
    ServerBuilder<?> serverBuilder =
        ServerBuilder.forPort(applicationProperties.getGrpc().getServer().getPort());
    serverBuilder
        .addService(ProtoReflectionService.newInstance())
        .addService(tracingServerInterceptor.intercept(onlineServingGrpcServiceV2));

    return serverBuilder.build();
  }
}
