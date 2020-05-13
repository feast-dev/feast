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
package feast.serving.configuration.redis;

import feast.core.StoreProto;
import feast.serving.specs.CachedSpecService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.*;

@Configuration
public class ServingStoreRedisConfig {

  @Bean
  StoreProto.Store.RedisConfig servingStoreRedisConf(CachedSpecService specService) {
    if (specService.getStore().getType() != StoreProto.Store.StoreType.REDIS) return null;
    return specService.getStore().getRedisConfig();
  }

  @Bean(destroyMethod = "shutdown")
  ClientResources servingClientResources() {
    return DefaultClientResources.create();
  }

  @Bean(destroyMethod = "shutdown")
  RedisClient servingRedisClient(
      ClientResources servingClientResources,
      ObjectProvider<StoreProto.Store.RedisConfig> servingStoreRedisConf) {
    if (servingStoreRedisConf.getIfAvailable() == null) return null;
    RedisURI redisURI =
        RedisURI.create(
            servingStoreRedisConf.getIfAvailable().getHost(),
            servingStoreRedisConf.getIfAvailable().getPort());
    return RedisClient.create(servingClientResources, redisURI);
  }

  @Bean(destroyMethod = "close")
  StatefulRedisConnection<byte[], byte[]> servingRedisConnection(
      ObjectProvider<RedisClient> servingRedisClient) {
    if (servingRedisClient.getIfAvailable() == null) return null;
    return servingRedisClient.getIfAvailable().connect(new ByteArrayCodec());
  }
}
