/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.service;

import com.google.common.annotations.VisibleForTesting;
import feast.serving.config.AppConfig;
import feast.specs.StorageSpecProto.StorageSpec;
import io.opentracing.Tracer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Service providing a mapping of storage ID and its {@link FeatureStorage}
 */
@Slf4j
public class FeatureStorageRegistry {

  private final Map<String, FeatureStorage> featureStorageMap = new ConcurrentHashMap<>();

  private final AppConfig appConfig;
  private final Tracer tracer;

  public FeatureStorageRegistry(AppConfig appConfig, Tracer tracer) {
    this.appConfig = appConfig;
    this.tracer = tracer;
  }

  /**
   * Get the feature storage associated with the given storage ID.
   *
   * @param storageId e.g. "REDIS1", "BIGTABLE2".
   * @return instance of the feature storage if exist. Otherwise return null.
   */
  public FeatureStorage get(String storageId) {
    return featureStorageMap.get(storageId);
  }

  /**
   * Connect to a feature storage defined by {@code storageSpec}. Currently supports Redis and
   * BigTable.
   *
   * @param storageSpec storage spec definition of the feature storage.
   * @return instance of the feature storage.
   * @throws UnsupportedOperationException if the storage type is not supported.
   */
  public FeatureStorage connect(StorageSpec storageSpec) {
    Map<String, String> options = storageSpec.getOptionsMap();
    FeatureStorage fs;

    if (storageSpec.getType().equals(BigTableFeatureStorage.TYPE)) {

      fs = new BigTableFeatureStorage(storageSpec);
      featureStorageMap.put(storageSpec.getId(), fs);
    } else if (storageSpec.getType().equals(RedisFeatureStorage.TYPE)) {
      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(appConfig.getRedisMaxPoolSize());
      poolConfig.setMaxIdle(appConfig.getRedisMaxIdleSize());
      JedisPool jedisPool =
          new JedisPool(
              poolConfig,
              options.get(RedisFeatureStorage.OPT_REDIS_HOST),
              Integer.valueOf(options.get(RedisFeatureStorage.OPT_REDIS_PORT)));
      fs = new RedisFeatureStorage(jedisPool, tracer);
      featureStorageMap.put(storageSpec.getId(), fs);
    } else {
      log.warn("Unknown storage: {}" + storageSpec);
      return null;
    }

    return fs;
  }

  @VisibleForTesting
  public void put(String storageId, FeatureStorage featureStorage) {
    featureStorageMap.put(storageId, featureStorage);
  }
}
