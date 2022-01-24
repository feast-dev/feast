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
package feast.serving.it;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import com.google.inject.*;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import feast.proto.serving.ServingServiceGrpc;
import feast.serving.config.*;
import feast.serving.grpc.OnlineServingGrpcServiceV2;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.util.MutableHandlerRegistry;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
abstract class ServingEnvironment {
  static DockerComposeContainer environment;

  ServingServiceGrpc.ServingServiceBlockingStub servingStub;
  Injector injector;
  String serverName;
  ManagedChannel channel;
  Server server;
  MutableHandlerRegistry serviceRegistry;

  static int serverPort = getFreePort();

  @BeforeAll
  static void globalSetup() {
    environment =
        new DockerComposeContainer(
                new File("src/test/resources/docker-compose/docker-compose-redis-it.yml"))
            .withExposedService("redis", 6379)
            .withExposedService("feast", 8080)
            .waitingFor("feast", Wait.forListeningPort());
    environment.start();
  }

  @AfterAll
  static void globalTeardown() {
    environment.stop();
  }

  @BeforeEach
  public void envSetUp() throws Exception {
    AbstractModule appPropertiesModule =
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(OnlineServingGrpcServiceV2.class);
          }

          @Provides
          ApplicationProperties applicationProperties() {
            final ApplicationProperties p = new ApplicationProperties();

            ApplicationProperties.GrpcServer grpcServer = new ApplicationProperties.GrpcServer();
            ApplicationProperties.Server server = new ApplicationProperties.Server();
            server.setPort(serverPort);
            grpcServer.setServer(server);
            p.setGrpc(grpcServer);

            final ApplicationProperties.FeastProperties feastProperties = createFeastProperties();
            feastProperties.setAwsRegion("us-east-1");
            p.setFeast(feastProperties);

            final ApplicationProperties.TracingProperties tracingProperties =
                new ApplicationProperties.TracingProperties();
            feastProperties.setTracing(tracingProperties);

            tracingProperties.setEnabled(false);
            return p;
          }
        };

    Module overrideConfig = registryConfig();
    Module registryConfig;
    if (overrideConfig != null) {
      registryConfig = Modules.override(new RegistryConfig()).with(registryConfig());
    } else {
      registryConfig = new RegistryConfig();
    }

    injector =
        Guice.createInjector(
            new ServingServiceConfigV2(),
            registryConfig,
            new InstrumentationConfig(),
            appPropertiesModule,
            new ServerModule());

    server = injector.getInstance(Server.class);
    server.start();

    channel = ManagedChannelBuilder.forAddress("localhost", serverPort).usePlaintext().build();

    servingStub =
        ServingServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(5, TimeUnit.SECONDS)
            .withWaitForReady();
  }

  @AfterEach
  public void envTeardown() throws Exception {
    // assume channel and server are not null
    channel.shutdown();
    server.shutdown();
    // fail the test if cannot gracefully shutdown
    try {
      assert channel.awaitTermination(5, TimeUnit.SECONDS)
          : "channel cannot be gracefully shutdown";
      assert server.awaitTermination(5, TimeUnit.SECONDS) : "server cannot be gracefully shutdown";
    } finally {
      channel.shutdownNow();
      server.shutdownNow();
    }

    server = null;
    channel = null;
    servingStub = null;
  }

  abstract ApplicationProperties.FeastProperties createFeastProperties();

  AbstractModule registryConfig() {
    return null;
  }

  private static int getFreePort() {
    ServerSocket serverSocket;
    try {
      serverSocket = new ServerSocket(0);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't allocate port");
    }

    assertThat(serverSocket, is(notNullValue()));
    assertThat(serverSocket.getLocalPort(), greaterThan(0));

    return serverSocket.getLocalPort();
  }
}
