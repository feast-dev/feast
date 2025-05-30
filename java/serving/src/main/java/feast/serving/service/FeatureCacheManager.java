/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2022 The Feast Authors
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
package feast.serving.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.serving.connectors.Feature;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FeatureCacheManager {
  private static final Logger log = LoggerFactory.getLogger(FeatureCacheManager.class);

  private final Cache<FeatureCacheKey, List<Feature>> cache;

  // TODO: Make these configurable via ApplicationProperties
  private static final long DEFAULT_MAXIMUM_SIZE = 100_000;
  private static final long DEFAULT_EXPIRE_AFTER_WRITE_MINUTES = 5;

  // Inner record for cache key to ensure proper equals and hashCode
  // It encapsulates the RedisKey and the specific feature references requested.
  // The order of featureReferences matters for the key.
  public record FeatureCacheKey(
      RedisProto.RedisKeyV2 redisKey,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FeatureCacheKey that = (FeatureCacheKey) o;
      // Using Message.equals for protobuf messages
      return Objects.equals(redisKey, that.redisKey)
          && Objects.equals(featureReferences, that.featureReferences);
    }

    @Override
    public int hashCode() {
      // Using Message.hashCode for protobuf messages
      return Objects.hash(redisKey, featureReferences);
    }
  }

  @Inject
  public FeatureCacheManager() {
    // Initialize Caffeine cache
    // In a real application, cache size and expiry would be configurable
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(DEFAULT_MAXIMUM_SIZE)
            .expireAfterWrite(DEFAULT_EXPIRE_AFTER_WRITE_MINUTES, TimeUnit.MINUTES)
            .build();
    log.info(
        "FeatureCacheManager initialized with maxSize={}, expireAfterWrite={}min",
        DEFAULT_MAXIMUM_SIZE,
        DEFAULT_EXPIRE_AFTER_WRITE_MINUTES);
  }

  /**
   * Retrieves a list of features from the cache.
   *
   * @param redisKey The Redis key associated with the entity.
   * @param featureReferences The list of feature references to retrieve.
   * @return A CompletableFuture that will complete with the list of features if found in cache, or
   *     null otherwise. The list of features itself can be null if the entry was explicitly cached
   *     as null (though not typical for this use case).
   */
  public CompletableFuture<List<Feature>> getFeatures(
      RedisProto.RedisKeyV2 redisKey,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    FeatureCacheKey cacheKey = new FeatureCacheKey(redisKey, featureReferences);
    List<Feature> features = cache.getIfPresent(cacheKey);
    // Return a completed future, OptimizedRedisOnlineRetriever will check if features is null
    return CompletableFuture.completedFuture(features);
  }

  /**
   * Puts a list of features into the cache.
   *
   * @param redisKey The Redis key associated with the entity.
   * @param featureReferences The list of feature references.
   * @param features The list of features to cache.
   */
  public void putFeatures(
      RedisProto.RedisKeyV2 redisKey,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      List<Feature> features) {
    if (features == null) {
      // Avoid caching nulls if it means "not found" vs "value is null"
      // For this cache, a null list means "not found in cache"
      return;
    }
    FeatureCacheKey cacheKey = new FeatureCacheKey(redisKey, featureReferences);
    cache.put(cacheKey, features);
  }

  /**
   * Invalidates a specific cache entry.
   *
   * @param redisKey The Redis key associated with the entity.
   * @param featureReferences The list of feature references for the specific entry.
   */
  public void invalidate(
      RedisProto.RedisKeyV2 redisKey,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    FeatureCacheKey cacheKey = new FeatureCacheKey(redisKey, featureReferences);
    cache.invalidate(cacheKey);
    log.debug("Invalidated cache for key: {}", cacheKey);
  }

  /**
   * Invalidates all cache entries associated with a given RedisKey. This is a more complex
   * operation as Caffeine does not directly support wildcard invalidation without iterating keys or
   * using external indexing. For simplicity, this method currently does nothing. A more robust
   * implementation might involve iterating cache keys if performance allows or using a different
   * caching strategy if this functionality is critical.
   *
   * <p>Alternatively, consider invalidating specific FeatureCacheKey entries when known.
   *
   * @param redisKey The base Redis key for which all associated entries should be invalidated.
   */
  public void invalidate(RedisProto.RedisKeyV2 redisKey) {
    // Caffeine doesn't directly support partial key invalidation easily.
    // One would need to iterate over cache.asMap().keySet() and remove matching keys,
    // which can be slow and not thread-safe without external locking.
    // For now, this method is a no-op or could log a warning.
    // A more targeted invalidation (using the full FeatureCacheKey) is preferred.
    log.warn(
        "Broad invalidation for RedisKey {} is not efficiently supported. "
            + "Consider invalidating specific FeatureCacheKeys.",
        redisKey.getProject()
            + ":"
            + redisKey.getEntityNamesList().toString()
            + ":"
            + redisKey.getEntityValuesList().toString());

    // If absolutely necessary, and understanding performance implications:
    // List<FeatureCacheKey> keysToInvalidate = new ArrayList<>();
    // cache.asMap().forEach((key, value) -> {
    //   if (key.redisKey().equals(redisKey)) {
    //     keysToInvalidate.add(key);
    //   }
    // });
    // cache.invalidateAll(keysToInvalidate);
    // log.debug("Attempted to invalidate {} entries for base RedisKey: {}",
    // keysToInvalidate.size(), redisKey);
  }

  /** Clears the entire cache. Use with caution. */
  public void clearAll() {
    cache.invalidateAll();
    log.info("Cleared all entries from FeatureCacheManager.");
  }

  public long estimatedSize() {
    return cache.estimatedSize();
  }
}
