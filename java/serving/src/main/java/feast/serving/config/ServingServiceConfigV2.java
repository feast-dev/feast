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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import feast.serving.registry.*;
import feast.serving.service.OnlineServingServiceV2;
import feast.serving.service.OnlineTransformationService;
import feast.serving.service.ServingServiceV2;
import feast.storage.api.retriever.OnlineRetrieverV2;
import feast.storage.connectors.redis.retriever.*;
import io.opentracing.Tracer;
import org.slf4j.Logger;

public class ServingServiceConfigV2 extends AbstractModule {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServingServiceConfigV2.class);

  @Provides
  public ServingServiceV2 registryBasedServingServiceV2(
      ApplicationProperties applicationProperties,
      RegistryRepository registryRepository,
      Tracer tracer) {
    final ServingServiceV2 servingService;
    final ApplicationProperties.Store store = applicationProperties.getFeast().getActiveStore();

    OnlineRetrieverV2 retrieverV2;
    // TODO: Support more store types, and potentially use a plugin model here.
    switch (store.getType()) {
      case REDIS_CLUSTER:
        RedisClientAdapter redisClusterClient =
            RedisClusterClient.create(store.getRedisClusterConfig());
        retrieverV2 =
            new OnlineRetriever(
                applicationProperties.getFeast().getProject(),
                redisClusterClient,
                new EntityKeySerializerV2());
        break;
      case REDIS:
        RedisClientAdapter redisClient = RedisClient.create(store.getRedisConfig());
        log.info("Created EntityKeySerializerV2");
        retrieverV2 =
            new OnlineRetriever(
                applicationProperties.getFeast().getProject(),
                redisClient,
                new EntityKeySerializerV2());
        break;
      default:
        throw new RuntimeException(
            String.format(
                "Unable to identify online store type: %s for Registry Backed Serving Service",
                store.getType()));
    }

    log.info("Working Directory = " + System.getProperty("user.dir"));

    final OnlineTransformationService onlineTransformationService =
        new OnlineTransformationService(
            applicationProperties.getFeast().getTransformationServiceEndpoint(),
            registryRepository);

    servingService =
        new OnlineServingServiceV2(
            retrieverV2,
            tracer,
            registryRepository,
            onlineTransformationService,
            applicationProperties.getFeast().getProject());

    return servingService;
  }
}
