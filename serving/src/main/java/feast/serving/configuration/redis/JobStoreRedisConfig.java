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
import feast.serving.FeastProperties;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobStoreRedisConfig {

  @Bean(destroyMethod = "shutdown")
  ClientResources jobStoreClientResources() {
    return DefaultClientResources.create();
  }

  @Bean(destroyMethod = "shutdown")
  RedisClient jobStoreRedisClient(
      ClientResources jobStoreClientResources, FeastProperties feastProperties) {
    try {
      if (StoreProto.Store.StoreType.valueOf(feastProperties.getJobs().getStoreType())
          != StoreProto.Store.StoreType.REDIS) return null;
      Map<String, String> jobStoreConf = feastProperties.getJobs().getStoreOptions();
      RedisURI uri =
          RedisURI.create(jobStoreConf.get("host"), Integer.parseInt(jobStoreConf.get("port")));
      return RedisClient.create(jobStoreClientResources, uri);
    } catch (Exception e) {
      // If the store type is empty or keys are not not properly set.
      return null;
    }
  }

  @Bean(destroyMethod = "close")
  StatefulRedisConnection<byte[], byte[]> jobStoreRedisConnection(
      ObjectProvider<RedisClient> jobStoreRedisClient) {
    if (jobStoreRedisClient.getIfAvailable() == null) return null;
    return jobStoreRedisClient.getIfAvailable().connect(new ByteArrayCodec());
  }
}
