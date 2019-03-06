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

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import feast.serving.exception.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** SpecStorage implementation with built-in in-memory cache. */
@Slf4j
public class CachedSpecStorage implements SpecStorage {
  private static final int MAX_SPEC_COUNT = 1000;

  private final CoreService coreService;
  private final LoadingCache<String, EntitySpec> entitySpecCache;
  private final CacheLoader<String, EntitySpec> entitySpecLoader;
  private final LoadingCache<String, FeatureSpec> featureSpecCache;
  private final CacheLoader<String, FeatureSpec> featureSpecLoader;
  private final LoadingCache<String, StorageSpec> storageSpecCache;
  private final CacheLoader<String, StorageSpec> storageSpecLoader;

  public CachedSpecStorage(
      CoreService coreService,
      ListeningExecutorService executorService,
      Duration cacheDuration,
      Ticker ticker) {
    this.coreService = coreService;
    entitySpecLoader =
        new CacheLoader<String, EntitySpec>() {
          @Override
          public EntitySpec load(String key) throws Exception {
            return coreService.getEntitySpecs(Collections.singletonList(key)).get(key);
          }

          @Override
          public ListenableFuture<EntitySpec> reload(String key, EntitySpec oldValue)
              throws Exception {
            return executorService.submit(
                () -> {
                  EntitySpec result = oldValue;
                  try {
                    result = coreService.getEntitySpecs(Collections.singleton(key)).get(key);
                  } catch (Exception e) {
                    log.error("Error reloading entity spec");
                  }
                  return result;
                });
          }
        };
    entitySpecCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_SPEC_COUNT)
            .refreshAfterWrite(cacheDuration)
            .ticker(ticker)
            .build(entitySpecLoader);

    featureSpecLoader =
        new CacheLoader<String, FeatureSpec>() {
          @Override
          public FeatureSpec load(String key) throws Exception {
            return coreService.getFeatureSpecs(Collections.singletonList(key)).get(key);
          }

          @Override
          public ListenableFuture<FeatureSpec> reload(String key, FeatureSpec oldValue)
              throws Exception {
            return executorService.submit(
                () -> {
                  FeatureSpec result = oldValue;
                  try {
                    result = coreService.getFeatureSpecs(Collections.singleton(key)).get(key);
                  } catch (Exception e) {
                    log.error("Error reloading feature spec");
                  }
                  return result;
                });
          }
        };
    featureSpecCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_SPEC_COUNT)
            .refreshAfterWrite(cacheDuration)
            .ticker(ticker)
            .build(featureSpecLoader);

    storageSpecLoader =
        new CacheLoader<String, StorageSpec>() {
          @Override
          public StorageSpec load(String key) throws Exception {
            return coreService.getStorageSpecs(Collections.singleton(key)).get(key);
          }

          @Override
          public ListenableFuture<StorageSpec> reload(String key, StorageSpec oldValue)
              throws Exception {
            return executorService.submit(
                () -> {
                  StorageSpec result = oldValue;
                  try {
                    result = coreService.getStorageSpecs(Collections.singleton(key)).get(key);
                  } catch (Exception e) {
                    log.error("Error reloading storage spec");
                  }
                  return result;
                });
          }
        };
    storageSpecCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_SPEC_COUNT)
            .refreshAfterWrite(cacheDuration)
            .ticker(ticker)
            .build(storageSpecLoader);
  }

  @Override
  public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
    try {
      return entitySpecCache.getAll(entityIds);
    } catch (Exception e) {
      log.error("Error while retrieving entity spec: {}", e);
      throw new SpecRetrievalException("Error while retrieving entity spec", e);
    }
  }

  @Override
  public Map<String, EntitySpec> getAllEntitySpecs() {
    try {
      Map<String, EntitySpec> result = coreService.getAllEntitySpecs();
      entitySpecCache.putAll(result);
      return result;
    } catch (Exception e) {
      log.error("Error while retrieving entity spec: {}", e);
      throw new SpecRetrievalException("Error while retrieving entity spec", e);
    }
  }

  @Override
  public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
    try {
      return featureSpecCache.getAll(featureIds);
    } catch (Exception e) {
      log.error("Error while retrieving feature spec: {}", e);
      throw new SpecRetrievalException("Error while retrieving feature spec", e);
    }
  }

  @Override
  public Map<String, FeatureSpec> getAllFeatureSpecs() {
    try {
      Map<String, FeatureSpec> result = coreService.getAllFeatureSpecs();
      featureSpecCache.putAll(result);
      return result;
    } catch (Exception e) {
      log.error("Error while retrieving feature spec: {}", e);
      throw new SpecRetrievalException("Error while retrieving feature spec", e);
    }
  }

  @Override
  public Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds) {
    try {
      return storageSpecCache.getAll(storageIds);
    } catch (Exception e) {
      log.error("Error while retrieving storage spec: {}", e);
      throw new SpecRetrievalException("Error while retrieving storage spec", e);
    }
  }

  @Override
  public Map<String, StorageSpec> getAllStorageSpecs() {
    try {
      Map<String, StorageSpec> result = coreService.getAllStorageSpecs();
      storageSpecCache.putAll(result);
      return result;
    } catch (Exception e) {
      log.error("Error while retrieving storage spec: {}", e);
      throw new SpecRetrievalException("Error while retrieving storage spec", e);
    }
  }

  @Override
  public boolean isConnected() {
    return coreService.isConnected();
  }

  /** Preload all spec into cache. */
  public void populateCache() {
    Map<String, FeatureSpec> featureSpecMap = coreService.getAllFeatureSpecs();
    featureSpecCache.putAll(featureSpecMap);

    Map<String, EntitySpec> entitySpecMap = coreService.getAllEntitySpecs();
    entitySpecCache.putAll(entitySpecMap);

    Map<String, StorageSpec> storageSpecMap = coreService.getAllStorageSpecs();
    storageSpecCache.putAll(storageSpecMap);
  }
}
