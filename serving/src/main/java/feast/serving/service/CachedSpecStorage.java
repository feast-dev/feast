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
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * SpecStorage implementation with built-in in-memory cache.
 */
@Slf4j
public class CachedSpecStorage implements SpecStorage {

  private static final int MAX_SPEC_COUNT = 1000;

  private final SpecStorage coreService;
  private final String storeId;

  private final CacheLoader<String, FeatureSetSpec> featureSetSpecCacheLoader;
  private final LoadingCache<String, FeatureSetSpec> featureSetSpecCache;
  private Store store;

  public CachedSpecStorage(SpecStorage coreService, String storeId) {
    this.storeId = storeId;
    this.coreService = coreService;
    this.store = coreService.getStoreDetails(storeId);

    featureSetSpecCacheLoader =
        CacheLoader.from(
            (String key) -> coreService.getFeatureSetSpecs(store.getSubscriptionsList()).get(key));
    featureSetSpecCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureSetSpecCacheLoader);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Store getStoreDetails(String id) {
    return store;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, FeatureSetSpec> getFeatureSetSpecs(List<Subscription> subscriptions) {
    return featureSetSpecCache.asMap();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isConnected() {
    return coreService.isConnected();
  }

  /**
   * Preload all spec into cache.
   */
  public void populateCache() {
    this.store = coreService.getStoreDetails(storeId);

    Map<String, FeatureSetSpec> featureSetSpecMap = coreService
        .getFeatureSetSpecs(store.getSubscriptionsList());
    featureSetSpecCache.putAll(featureSetSpecMap);
  }
}
