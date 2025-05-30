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
package feast.serving.connectors.redis.retriever;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.serving.connectors.Feature;
import feast.serving.connectors.OnlineRetriever;
import feast.serving.connectors.redis.common.RedisHashDecoder;
import feast.serving.connectors.redis.common.RedisKeyGenerator;
import feast.serving.service.FeatureCacheManager; // To be created
import feast.serving.util.RedisPerformanceMonitor; // To be created
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OptimizedRedisOnlineRetriever implements OnlineRetriever {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(OptimizedRedisOnlineRetriever.class);

  private static final String TIMESTAMP_PREFIX = "_ts";
  private final OptimizedRedisClientAdapter redisClientAdapter; // To be created
  private final EntityKeySerializer keySerializer;
  private final String project;
  private final FeatureCacheManager featureCacheManager;
  private final RedisPerformanceMonitor performanceMonitor;
  private final ExecutorService executorService;

  // Adaptive threshold parameters
  private volatile int adaptiveHgetallThreshold = 50; // Default, can be tuned
  private static final int MIN_HGETALL_THRESHOLD = 10;
  private static final int MAX_HGETALL_THRESHOLD = 200;

  public OptimizedRedisOnlineRetriever(
      String project,
      OptimizedRedisClientAdapter redisClientAdapter,
      EntityKeySerializer keySerializer,
      FeatureCacheManager featureCacheManager,
      RedisPerformanceMonitor performanceMonitor,
      int threadPoolSize) {
    this.project = project;
    this.redisClientAdapter = redisClientAdapter;
    this.keySerializer = keySerializer;
    this.featureCacheManager = featureCacheManager;
    this.performanceMonitor = performanceMonitor;
    this.executorService = Executors.newFixedThreadPool(threadPoolSize > 0 ? threadPoolSize : Runtime.getRuntime().availableProcessors());
  }

  @Override
  public List<List<Feature>> getOnlineFeatures(
      List<Map<String, ValueProto.Value>> entityRows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      List<String> entityNames) {

    if (entityRows.isEmpty() || featureReferences.isEmpty()) {
      return Collections.emptyList();
    }

    // Initialize results structure
    List<List<Feature>> finalResults =
        Lists.newArrayListWithCapacity(entityRows.size());
    for (int i = 0; i < entityRows.size(); i++) {
      finalResults.add(Lists.newArrayListWithCapacity(featureReferences.size()));
    }

    // Step 1: Try to fetch from cache
    List<CompletableFuture<List<Feature>>> cachedFutures = Lists.newArrayList();
    List<Integer> cacheMissIndices = Lists.newArrayList();
    List<Map<String, ValueProto.Value>> cacheMissEntityRows = Lists.newArrayList();

    for (int i = 0; i < entityRows.size(); i++) {
      Map<String, ValueProto.Value> entityRow = entityRows.get(i);
      RedisProto.RedisKeyV2 redisKey = RedisKeyGenerator.buildRedisKey(this.project, entityRow);
      CompletableFuture<List<Feature>> cachedResult =
          featureCacheManager.getFeatures(redisKey, featureReferences);
      cachedFutures.add(cachedResult);
    }
    
    CompletableFuture.allOf(cachedFutures.toArray(new CompletableFuture[0])).join();

    for (int i = 0; i < cachedFutures.size(); i++) {
        try {
            List<Feature> features = cachedFutures.get(i).get(); // Should be completed
            if (features != null && !features.isEmpty() && features.stream().allMatch(Objects::nonNull)) {
                finalResults.set(i, features);
            } else {
                cacheMissIndices.add(i);
                cacheMissEntityRows.add(entityRows.get(i));
            }
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Error fetching from cache for entity row index {}: {}", i, e.getMessage());
            cacheMissIndices.add(i);
            cacheMissEntityRows.add(entityRows.get(i));
        }
    }

    if (cacheMissEntityRows.isEmpty()) {
      return finalResults;
    }

    // Step 2: Fetch cache misses from Redis
    List<RedisProto.RedisKeyV2> redisKeysForMisses =
        RedisKeyGenerator.buildRedisKeys(this.project, cacheMissEntityRows);

    List<List<Feature>> redisResults =
        getFeaturesFromRedisPipelined(redisKeysForMisses, featureReferences);

    // Step 3: Populate final results and update cache
    for (int i = 0; i < redisResults.size(); i++) {
      int originalIndex = cacheMissIndices.get(i);
      List<Feature> features = redisResults.get(i);
      finalResults.set(originalIndex, features);
      if (features != null && !features.isEmpty()) {
        featureCacheManager.putFeatures(redisKeysForMisses.get(i), featureReferences, features);
      }
    }
    return finalResults;
  }

  private List<List<Feature>> getFeaturesFromRedisPipelined(
      List<RedisProto.RedisKeyV2> redisKeys,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {

    long startTime = System.nanoTime();
    
    Map<ByteBuffer, Integer> byteToFeatureIdxMap = new HashMap<>();
    List<byte[]> retrieveFieldsBinary = new ArrayList<>();

    for (int idx = 0; idx < featureReferences.size(); idx++) {
      byte[] featureReferenceBytes =
          RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReferences.get(idx));
      retrieveFieldsBinary.add(featureReferenceBytes);
      byteToFeatureIdxMap.put(ByteBuffer.wrap(featureReferenceBytes), idx);
    }

    featureReferences.stream()
        .map(ServingAPIProto.FeatureReferenceV2::getFeatureViewName)
        .distinct()
        .forEach(
            table -> {
              byte[] featureTableTsBytes =
                  RedisHashDecoder.getTimestampRedisHashKeyBytes(table, TIMESTAMP_PREFIX);
              retrieveFieldsBinary.add(featureTableTsBytes);
            });

    byte[][] retrieveFieldsByteArray = retrieveFieldsBinary.toArray(new byte[0][0]);
    List<byte[]> binaryRedisKeys =
        redisKeys.stream().map(this.keySerializer::serialize).collect(Collectors.toList());

    List<CompletableFuture<Map<byte[], byte[]>>> futures =
        Lists.newArrayListWithCapacity(binaryRedisKeys.size());

    RedisAsyncCommands<byte[], byte[]> asyncCommands = redisClientAdapter.getAsyncCommands();
    asyncCommands.setAutoFlushCommands(false); // Enable pipelining

    boolean useHmget = shouldUseHmget(retrieveFieldsBinary.size());

    for (byte[] binaryRedisKey : binaryRedisKeys) {
      RedisFuture<List<KeyValue<byte[], byte[]>>> futureList;
      RedisFuture<Map<byte[], byte[]>> futureMap;

      if (useHmget) {
        futureList = asyncCommands.hmget(binaryRedisKey, retrieveFieldsByteArray);
        futures.add(
            futureList.thenApply(
                list ->
                    list.stream()
                        .filter(KeyValue::hasValue)
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))
                .toCompletableFuture());
      } else {
        futureMap = asyncCommands.hgetall(binaryRedisKey);
        futures.add(futureMap.toCompletableFuture());
      }
    }
    asyncCommands.flushCommands(); // Execute all commands in pipeline
    redisClientAdapter.releaseAsyncCommands(asyncCommands);


    List<List<Feature>> results = Lists.newArrayListWithCapacity(futures.size());
    for (CompletableFuture<Map<byte[], byte[]>> f : futures) {
      try {
        results.add(
            RedisHashDecoder.retrieveFeature(
                f.get(), byteToFeatureIdxMap, featureReferences, TIMESTAMP_PREFIX));
      } catch (InterruptedException | ExecutionException e) {
        log.error("Exception occurred while fetching features from Redis pipeline: {}", e.getMessage(), e);
        // Add empty list or handle error appropriately
        results.add(Collections.nCopies(featureReferences.size(), null));
      }
    }

    long durationNanos = System.nanoTime() - startTime;
    performanceMonitor.recordRedisOperation(
        useHmget ? "hmget_pipelined" : "hgetall_pipelined",
        binaryRedisKeys.size(),
        retrieveFieldsBinary.size(),
        durationNanos);
    
    // Simple adaptive logic for threshold (can be made more sophisticated)
    if (useHmget) {
        performanceMonitor.recordHmgetLatency(durationNanos / binaryRedisKeys.size());
    } else {
        performanceMonitor.recordHgetallLatency(durationNanos / binaryRedisKeys.size());
    }
    updateAdaptiveThreshold();

    return results;
  }

  private boolean shouldUseHmget(int fieldCount) {
    // This can be enhanced with more sophisticated adaptive logic using performanceMonitor stats
    return fieldCount < adaptiveHgetallThreshold;
  }
  
  private void updateAdaptiveThreshold() {
      // Example: Adjust threshold based on recent average latencies
      // This is a placeholder for a more robust adaptive algorithm
      double avgHmgetLatency = performanceMonitor.getAverageHmgetLatency();
      double avgHgetallLatency = performanceMonitor.getAverageHgetallLatency();

      if (avgHmgetLatency > 0 && avgHgetallLatency > 0) {
          // If HMGET is significantly slower for current threshold, consider increasing it
          if (avgHmgetLatency > avgHgetallLatency * 1.2 && adaptiveHgetallThreshold < MAX_HGETALL_THRESHOLD) {
              adaptiveHgetallThreshold = Math.min(MAX_HGETALL_THRESHOLD, (int)(adaptiveHgetallThreshold * 1.1));
          } 
          // If HGETALL is slower, consider decreasing threshold
          else if (avgHgetallLatency > avgHmgetLatency * 1.2 && adaptiveHgetallThreshold > MIN_HGETALL_THRESHOLD) {
              adaptiveHgetallThreshold = Math.max(MIN_HGETALL_THRESHOLD, (int)(adaptiveHgetallThreshold * 0.9));
          }
      }
      // log.debug("Updated adaptive HGETALL threshold to: {}", adaptiveHgetallThreshold);
  }


  @Override
  public boolean deleteStaleFeature(
      ServingAPIProto.FeatureReferenceV2 featureReference,
      Map<String, ValueProto.Value> entityRow) {
    long startTime = System.nanoTime();
    try {
      RedisProto.RedisKeyV2 redisKey = RedisKeyGenerator.buildRedisKey(this.project, entityRow);
      if (redisKey == null) {
        log.warn("Could not build Redis key for entity row: {}", entityRow);
        return false;
      }

      byte[] binaryRedisKey = this.keySerializer.serialize(redisKey);
      byte[] featureReferenceBytes =
          RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReference);

      RedisAsyncCommands<byte[], byte[]> asyncCommands = redisClientAdapter.getAsyncCommands();
      RedisFuture<Long> hdelFuture = asyncCommands.hdel(binaryRedisKey, featureReferenceBytes);
      redisClientAdapter.releaseAsyncCommands(asyncCommands);

      Long result = hdelFuture.get(); // Blocking call, but for a single op

      long durationNanos = System.nanoTime() - startTime;
      performanceMonitor.recordRedisOperation("hdel", 1, 1, durationNanos);

      if (result > 0) {
        log.info(
            "Successfully deleted stale feature {} for entity with key {}",
            featureReference.getFeatureViewName() + ":" + featureReference.getFeatureName(),
            entityRow);
        // Invalidate cache for this key if deletion is successful
        featureCacheManager.invalidate(redisKey);
        return true;
      } else {
        log.warn(
            "Failed to delete stale feature, key or field does not exist: {}",
            featureReference.getFeatureViewName() + ":" + featureReference.getFeatureName());
        return false;
      }
    } catch (Exception e) {
      log.error("Error deleting stale feature from Redis: {}", e.getMessage(), e);
      long durationNanos = System.nanoTime() - startTime;
      performanceMonitor.recordRedisError("hdel");
      performanceMonitor.recordRedisOperation("hdel_error", 1, 1, durationNanos);
      return false;
    }
  }

  // Shutdown executor service when this retriever is no longer needed
  public void shutdown() {
    try {
      log.info("Shutting down OptimizedRedisOnlineRetriever executor service.");
      executorService.shutdown();
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
