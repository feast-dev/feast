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
import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.proto.storage.RedisProto.RedisKeyV2;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import feast.storage.connectors.redis.common.RedisHashDecoder;
import feast.storage.connectors.redis.common.RedisKeyGenerator;
import io.grpc.Status;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class RedisOnlineRetrieverV2 implements OnlineRetrieverV2 {

  private static final String timestampPrefix = "_ts";
  private final RedisAsyncCommands<byte[], byte[]> asyncCommands;

  private RedisOnlineRetrieverV2(StatefulRedisConnection<byte[], byte[]> connection) {
    this.asyncCommands = connection.async();
  }

  public static OnlineRetrieverV2 create(Map<String, String> config) {

    StatefulRedisConnection<byte[], byte[]> connection =
        RedisClient.create(
                RedisURI.create(config.get("host"), Integer.parseInt(config.get("port"))))
            .connect(new ByteArrayCodec());

    return new RedisOnlineRetrieverV2(connection);
  }

  public static OnlineRetrieverV2 create(StatefulRedisConnection<byte[], byte[]> connection) {
    return new RedisOnlineRetrieverV2(connection);
  }

  @Override
  public List<List<Optional<Feature>>> getOnlineFeatures(
      String project, List<EntityRow> entityRows, List<FeatureReferenceV2> featureReferences) {

    List<RedisKeyV2> redisKeys = RedisKeyGenerator.buildRedisKeys(project, entityRows);
    List<List<Optional<Feature>>> features = getFeaturesFromRedis(redisKeys, featureReferences);

    return features;
  }

  private List<List<Optional<Feature>>> getFeaturesFromRedis(
      List<RedisKeyV2> redisKeys, List<FeatureReferenceV2> featureReferences) {
    List<List<Optional<Feature>>> features = new ArrayList<>();
    // To decode bytes back to Feature Reference
    Map<String, FeatureReferenceV2> byteToFeatureReferenceMap = new HashMap<>();
    // To check whether redis ValueK is a timestamp field
    Map<String, Boolean> isTimestampMap = new HashMap<>();

    // Serialize using proto
    List<byte[]> binaryRedisKeys =
        redisKeys.stream().map(redisKey -> redisKey.toByteArray()).collect(Collectors.toList());

    List<byte[]> featureReferenceWithTsByteList = new ArrayList<>();
    featureReferences.stream()
        .forEach(
            featureReference -> {

              // eg. murmur(<featuretable_name:feature_name>)
              String delimitedFeatureReference =
                  featureReference.getFeatureTable() + ":" + featureReference.getName();
              byte[] featureReferenceBytes =
                  Hashing.murmur3_32()
                      .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
                      .asBytes();
              featureReferenceWithTsByteList.add(featureReferenceBytes);
              isTimestampMap.put(Arrays.toString(featureReferenceBytes), false);
              byteToFeatureReferenceMap.put(featureReferenceBytes.toString(), featureReference);

              // eg. <_ts:featuretable_name>
              byte[] featureTableTsBytes =
                  RedisHashDecoder.getTimestampRedisHashKeyBytes(featureReference, timestampPrefix);
              isTimestampMap.put(Arrays.toString(featureTableTsBytes), true);
              featureReferenceWithTsByteList.add(featureTableTsBytes);
            });

    // Disable auto-flushing
    asyncCommands.setAutoFlushCommands(false);

    // Perform a series of independent calls
    List<RedisFuture<List<KeyValue<byte[], byte[]>>>> futures = Lists.newArrayList();
    for (byte[] binaryRedisKey : binaryRedisKeys) {
      byte[][] featureReferenceWithTsByteArrays =
          featureReferenceWithTsByteList.toArray(new byte[0][]);
      // Access redis keys and extract features
      futures.add(asyncCommands.hmget(binaryRedisKey, featureReferenceWithTsByteArrays));
    }

    // Write all commands to the transport layer
    asyncCommands.flushCommands();

    futures.forEach(
        future -> {
          try {
            List<KeyValue<byte[], byte[]>> redisValuesList = future.get();
            List<Optional<Feature>> curRedisKeyFeatures =
                RedisHashDecoder.retrieveFeature(
                    redisValuesList, isTimestampMap, byteToFeatureReferenceMap, timestampPrefix);
            features.add(curRedisKeyFeatures);
          } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
            throw Status.UNKNOWN
                .withDescription("Unexpected error when pulling data from from Redis.")
                .withCause(e)
                .asRuntimeException();
          }
        });
    return features;
  }
}
