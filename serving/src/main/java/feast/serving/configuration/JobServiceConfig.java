/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.serving.configuration;

import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisBackedJobService;
import feast.serving.specs.CachedSpecService;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class JobServiceConfig {

  public static final String DEFAULT_REDIS_MAX_CONN = "8";
  public static final String DEFAULT_REDIS_MAX_IDLE = "8";
  public static final String DEFAULT_REDIS_MAX_WAIT_MILLIS = "50";

  @Bean
  public JobService jobService(FeastProperties feastProperties, CachedSpecService specService) {
    if (!specService.getStore().getType().equals(StoreType.BIGQUERY)) {
      return new NoopJobService();
    }
    StoreType storeType = StoreType.valueOf(feastProperties.getJobs().getStoreType());
    Map<String, String> storeOptions = feastProperties.getJobs().getStoreOptions();
    switch (storeType) {
      case REDIS:
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(
            Integer.parseInt(storeOptions.getOrDefault("max-conn", DEFAULT_REDIS_MAX_CONN)));
        jedisPoolConfig.setMaxIdle(
            Integer.parseInt(storeOptions.getOrDefault("max-idle", DEFAULT_REDIS_MAX_IDLE)));
        jedisPoolConfig.setMaxWaitMillis(
            Integer.parseInt(
                storeOptions.getOrDefault("max-wait-millis", DEFAULT_REDIS_MAX_WAIT_MILLIS)));
        JedisPool jedisPool =
            new JedisPool(
                jedisPoolConfig,
                storeOptions.get("host"),
                Integer.parseInt(storeOptions.get("port")));
        return new RedisBackedJobService(jedisPool);
      case INVALID:
      case BIGQUERY:
      case CASSANDRA:
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported store type '%s' for job store", storeType));
    }
  }
}
