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
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import feast.storage.connectors.redis.common.RedisHashDecoder;
import feast.storage.connectors.redis.serializer.RedisKeyPrefixSerializerV2;
import feast.storage.connectors.redis.serializer.RedisKeySerializerV2;
import io.grpc.Status;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Defines a storage retriever */
public class RedisClusterOnlineRetrieverV2 implements OnlineRetrieverV2 {

  private static final String timestampPrefix = "_ts";
  private final RedisAdvancedClusterAsyncCommands<byte[], byte[]> asyncCommands;
  private final RedisKeySerializerV2 serializer;
  @Nullable private final RedisKeySerializerV2 fallbackSerializer;

  static class Builder {
    private final StatefulRedisClusterConnection<byte[], byte[]> connection;
    private final RedisKeySerializerV2 serializer;
    @Nullable private RedisKeySerializerV2 fallbackSerializer;

    Builder(
        StatefulRedisClusterConnection<byte[], byte[]> connection,
        RedisKeySerializerV2 serializer) {
      this.connection = connection;
      this.serializer = serializer;
    }

    Builder withFallbackSerializer(RedisKeySerializerV2 fallbackSerializer) {
      this.fallbackSerializer = fallbackSerializer;
      return this;
    }

    RedisClusterOnlineRetrieverV2 build() {
      return new RedisClusterOnlineRetrieverV2(this);
    }
  }

  private RedisClusterOnlineRetrieverV2(Builder builder) {
    this.asyncCommands = builder.connection.async();
    this.serializer = builder.serializer;
    this.fallbackSerializer = builder.fallbackSerializer;

    // Disable auto-flushing
    this.asyncCommands.setAutoFlushCommands(false);
  }

  public static OnlineRetrieverV2 create(Map<String, String> config) {
    List<RedisURI> redisURIList =
        Arrays.stream(config.get("connection_string").split(","))
            .map(
                hostPort -> {
                  String[] hostPortSplit = hostPort.trim().split(":");
                  return RedisURI.create(hostPortSplit[0], Integer.parseInt(hostPortSplit[1]));
                })
            .collect(Collectors.toList());
    StatefulRedisClusterConnection<byte[], byte[]> connection =
        RedisClusterClient.create(redisURIList).connect(new ByteArrayCodec());

    RedisKeySerializerV2 serializer =
        new RedisKeyPrefixSerializerV2(config.getOrDefault("key_prefix", ""));

    Builder builder = new Builder(connection, serializer);

    if (Boolean.parseBoolean(config.getOrDefault("enable_fallback", "false"))) {
      RedisKeySerializerV2 fallbackSerializer =
          new RedisKeyPrefixSerializerV2(config.getOrDefault("fallback_prefix", ""));
      builder = builder.withFallbackSerializer(fallbackSerializer);
    }

    return builder.build();
  }

  @Override
  public List<List<Optional<Feature>>> getOnlineFeatures(
      String project,
      List<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {

    List<RedisProto.RedisKeyV2> redisKeys = buildRedisKeys(project, entityRows);
    List<List<Optional<Feature>>> features = getFeaturesFromRedis(redisKeys, featureReferences);

    return features;
  }

  private List<RedisProto.RedisKeyV2> buildRedisKeys(
      String project, List<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows) {
    List<RedisProto.RedisKeyV2> redisKeys =
        entityRows.stream()
            .map(entityRow -> makeRedisKey(project, entityRow))
            .collect(Collectors.toList());

    return redisKeys;
  }

  /**
   * Create {@link RedisProto.RedisKeyV2}
   *
   * @param project Project where request for features was called from
   * @param entityRow {@link ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow}
   * @return {@link RedisProto.RedisKeyV2}
   */
  private RedisProto.RedisKeyV2 makeRedisKey(
      String project, ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow entityRow) {
    RedisProto.RedisKeyV2.Builder builder = RedisProto.RedisKeyV2.newBuilder().setProject(project);
    Map<String, ValueProto.Value> fieldsMap = entityRow.getFieldsMap();
    List<String> entityNames = new ArrayList<>(new HashSet<>(fieldsMap.keySet()));

    // Sort entity names by alphabetical order
    entityNames.sort(String::compareTo);

    for (String entityName : entityNames) {
      builder.addEntityNames(entityName);
      builder.addEntityValues(fieldsMap.get(entityName));
    }
    return builder.build();
  }

  private List<List<Optional<Feature>>> getFeaturesFromRedis(
      List<RedisProto.RedisKeyV2> redisKeys,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    List<List<Optional<Feature>>> features = new ArrayList<>();
    // To decode bytes back to Feature Reference
    Map<String, ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap = new HashMap<>();

    // Serialize using proto
    List<byte[]> binaryRedisKeys =
        redisKeys.stream().map(redisKey -> redisKey.toByteArray()).collect(Collectors.toList());

    List<byte[]> featureReferenceWithTsByteList = new ArrayList<>();
    featureReferences.stream()
        .forEach(
            featureReference -> {

              // eg. murmur(<featuretable_name:feature_name>)
              byte[] featureReferenceBytes =
                  RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReference);
              featureReferenceWithTsByteList.add(featureReferenceBytes);
              byteToFeatureReferenceMap.put(featureReferenceBytes.toString(), featureReference);

              // eg. <_ts:featuretable_name>
              byte[] featureTableTsBytes =
                  RedisHashDecoder.getTimestampRedisHashKeyBytes(featureReference, timestampPrefix);
              featureReferenceWithTsByteList.add(featureTableTsBytes);
            });

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

    // TODO: Add in redis key prefix logic
    futures.forEach(
        future -> {
          try {
            List<KeyValue<byte[], byte[]>> redisValuesList = future.get();
            List<Optional<Feature>> curRedisKeyFeatures =
                RedisHashDecoder.retrieveFeature(
                    redisValuesList, byteToFeatureReferenceMap, timestampPrefix);
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
