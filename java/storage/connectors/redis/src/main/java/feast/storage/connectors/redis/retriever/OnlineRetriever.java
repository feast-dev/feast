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
package feast.storage.connectors.redis.retriever;

import com.google.common.collect.Lists;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import feast.storage.connectors.redis.common.RedisHashDecoder;
import feast.storage.connectors.redis.common.RedisKeyGenerator;
import io.lettuce.core.KeyValue;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class OnlineRetriever implements OnlineRetrieverV2 {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineRetriever.class);

  private static final String timestampPrefix = "_ts";
  private final RedisClientAdapter redisClientAdapter;
  private final EntityKeySerializer keySerializer;
  private final String project;

  // Number of fields in request to Redis which requires using HGETALL instead of HMGET
  public static final int HGETALL_NUMBER_OF_FIELDS_THRESHOLD = 50;

  public OnlineRetriever(
      String project, RedisClientAdapter redisClientAdapter, EntityKeySerializer keySerializer) {
    this.project = project;
    this.redisClientAdapter = redisClientAdapter;
    this.keySerializer = keySerializer;
  }

  @Override
  public List<List<Feature>> getOnlineFeatures(
      List<Map<String, ValueProto.Value>> entityRows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      Map<String, List<String>> entityNamesPerFeatureView) {

    Set<String> featureViewNames = entityNamesPerFeatureView.keySet();

    Set<List<String>> uniqueEntityKeyCombinations =
        featureViewNames.stream().map(entityNamesPerFeatureView::get).collect(Collectors.toSet());

    List<List<RedisProto.RedisKeyV2>> redisKeys =
        entityRows.stream()
            .map(
                entityRow ->
                    uniqueEntityKeyCombinations.stream()
                        .map(
                            entitiesToRetain -> {
                              Map<String, Value> res = new HashMap<>(entityRow);
                              res.keySet().retainAll(entitiesToRetain);
                              return res;
                            })
                        .collect(Collectors.toList()))
            .map(es -> RedisKeyGenerator.buildRedisKeys(this.project, es))
            .collect(Collectors.toList());

    return getFeaturesFromRedis(redisKeys, featureReferences);
  }

  private List<List<Feature>> getFeaturesFromRedis(
      List<List<RedisProto.RedisKeyV2>> redisKeys,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    // To decode bytes back to Feature
    Map<ByteBuffer, Integer> byteToFeatureIdxMap = new HashMap<>();

    // Serialize using proto
    List<List<byte[]>> binaryRedisKeys =
        redisKeys.stream()
            .map(l -> l.stream().map(this.keySerializer::serialize).collect(Collectors.toList()))
            .collect(Collectors.toList());

    List<byte[]> retrieveFields = new ArrayList<>();
    for (int idx = 0;
        idx < featureReferences.size();
        idx++) { // eg. murmur(<featuretable_name:feature_name>)
      byte[] featureReferenceBytes =
          RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReferences.get(idx));
      retrieveFields.add(featureReferenceBytes);

      byteToFeatureIdxMap.put(ByteBuffer.wrap(featureReferenceBytes), idx);
    }

    featureReferences.stream()
        .map(ServingAPIProto.FeatureReferenceV2::getFeatureViewName)
        .distinct()
        .forEach(
            table -> {
              // eg. <_ts:featuretable_name>
              byte[] featureTableTsBytes =
                  RedisHashDecoder.getTimestampRedisHashKeyBytes(table, timestampPrefix);

              retrieveFields.add(featureTableTsBytes);
            });

    List<Future<Map<byte[], byte[]>>> futures =
        Lists.newArrayListWithExpectedSize(binaryRedisKeys.size());

    for (List<byte[]> binaryRedisKeysForEntity : binaryRedisKeys) {
      List<CompletableFuture<Map<byte[], byte[]>>> innerFutures =
          Lists.newArrayListWithExpectedSize(binaryRedisKeysForEntity.size());

      // Number of fields that controls whether to use hmget or hgetall was discovered empirically
      // Could be potentially tuned further
      if (retrieveFields.size() < HGETALL_NUMBER_OF_FIELDS_THRESHOLD) {
        byte[][] retrieveFieldsByteArray = retrieveFields.toArray(new byte[0][]);
        for (byte[] binaryRedisKey : binaryRedisKeysForEntity) {
          innerFutures.add(
              redisClientAdapter
                  .hmget(binaryRedisKey, retrieveFieldsByteArray)
                  .thenApply(
                      list ->
                          list.stream()
                              .filter(KeyValue::hasValue)
                              .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))
                  .toCompletableFuture());
        }
      } else {
        for (byte[] binaryRedisKey : binaryRedisKeysForEntity) {
          innerFutures.add(redisClientAdapter.hgetall(binaryRedisKey).toCompletableFuture());
        }
      }
      futures.add(mergeFuturesForEntity(innerFutures));
    }

    List<List<Feature>> results = Lists.newArrayListWithExpectedSize(futures.size());
    for (Future<Map<byte[], byte[]>> f : futures) {
      try {
        results.add(
            RedisHashDecoder.retrieveFeature(
                f.get(), byteToFeatureIdxMap, featureReferences, timestampPrefix));
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Unexpected error when pulling data from Redis");
      }
    }

    return results;
  }

  private Future<Map<byte[], byte[]>> mergeFuturesForEntity(
      List<CompletableFuture<Map<byte[], byte[]>>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
        .thenApply(
            featureMappings -> {
              Map<ByteBuffer, byte[]> mergedMapping = new HashMap<>();
              featureMappings.forEach(
                  featureMapping ->
                      featureMapping.forEach(
                          (key, value) -> {
                            ByteBuffer wrappedKey = ByteBuffer.wrap(key);
                            if (!mergedMapping.containsKey(wrappedKey)) {
                              mergedMapping.put(wrappedKey, value);
                            }
                          }));
              return mergedMapping.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey().array(), Entry::getValue));
            });
  }
}
