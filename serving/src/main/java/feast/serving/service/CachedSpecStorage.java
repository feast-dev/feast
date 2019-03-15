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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import feast.serving.exception.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * SpecStorage implementation with built-in in-memory cache.
 */
@Slf4j
public class CachedSpecStorage implements SpecStorage {

  private static final int MAX_SPEC_COUNT = 1000;

  private final CoreService coreService;
  private final LoadingCache<String, EntitySpec> entitySpecCache;
  private final CacheLoader<String, EntitySpec> entitySpecLoader;
  private final LoadingCache<String, FeatureSpec> featureSpecCache;
  private final CacheLoader<String, FeatureSpec> featureSpecLoader;
  private StorageSpec storageSpec;

  public CachedSpecStorage(CoreService coreService) {
    this.coreService = coreService;
    entitySpecLoader =
        CacheLoader.from(
            (String key) -> coreService.getEntitySpecs(Collections.singletonList(key)).get(key));
    entitySpecCache = CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(entitySpecLoader);

    featureSpecLoader =
        CacheLoader.from(
            (String key) -> coreService.getFeatureSpecs(Collections.singletonList(key)).get(key));
    featureSpecCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureSpecLoader);
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
  public StorageSpec getServingStorageSpec() {
    if (storageSpec == null) {
      try {
        storageSpec = coreService.getServingStorageSpec();
      } catch (Exception e) {
        log.error("Error while retrieving storage spec: {}", e);
        throw new SpecRetrievalException("Error while retrieving storage spec", e);
      }
    }
    return storageSpec;
  }

  @Override
  public boolean isConnected() {
    return coreService.isConnected();
  }

  /**
   * Preload all spec into cache.
   */
  public void populateCache() {
    Map<String, FeatureSpec> featureSpecMap = coreService.getAllFeatureSpecs();
    featureSpecCache.putAll(featureSpecMap);

    Map<String, EntitySpec> entitySpecMap = coreService.getAllEntitySpecs();
    entitySpecCache.putAll(entitySpecMap);

    getServingStorageSpec();
  }
}
