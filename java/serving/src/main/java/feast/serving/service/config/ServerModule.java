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
package feast.serving.service.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import feast.serving.service.ServingServiceV2;
import feast.serving.service.controller.HealthServiceController;
import feast.serving.service.grpc.OnlineServingGrpcServiceV2;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentracing.contrib.grpc.TracingServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ServerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(OnlineServingGrpcServiceV2.class);
    }

    @Provides
    public Server provideGrpcServer(
            ApplicationProperties applicationProperties,
            OnlineServingGrpcServiceV2 onlineServingGrpcServiceV2,
            TracingServerInterceptor tracingServerInterceptor,
            HealthGrpc.HealthImplBase healthImplBase) {

        // Create a CachedThreadPool executor
        ExecutorService executorService = Executors.newCachedThreadPool();

        // Log details about the thread pool
        logThreadPoolDetails(executorService);

        ServerBuilder<?> serverBuilder =
                ServerBuilder.forPort(applicationProperties.getGrpc().getServer().getPort()).executor(executorService);
        serverBuilder
                .addService(ProtoReflectionService.newInstance())
                .addService(tracingServerInterceptor.intercept(onlineServingGrpcServiceV2))
                .addService(healthImplBase);

        return serverBuilder.build();
    }

    private void logThreadPoolDetails(ExecutorService executorService) {
        if (executorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;

            // Log relevant details about the thread pool
            Logger logger = LoggerFactory.getLogger(getClass());

            int corePoolSize = threadPoolExecutor.getCorePoolSize();
            int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
            long keepAliveTime = threadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS);

            // Check if it's a CachedThreadPool (maximumPoolSize == Integer.MAX_VALUE means it's unbounded)
            if (maximumPoolSize == Integer.MAX_VALUE) {
                logger.info("Using CachedThreadPool with core pool size: {}, keep-alive time: {} seconds",
                        corePoolSize, keepAliveTime);
            } else {
                logger.info("Using FixedThreadPool with core pool size: {}, max pool size: {}, keep-alive time: {} seconds",
                        corePoolSize, maximumPoolSize, keepAliveTime);
            }
        } else {
            // Log if it's not a thread pool executor
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.warn("Executor is not a ThreadPoolExecutor, it's: {}", executorService.getClass().getName());
        }
    }

    @Provides
    public HealthGrpc.HealthImplBase healthService(ServingServiceV2 servingServiceV2) {
        return new HealthServiceController(servingServiceV2);
    }
}
