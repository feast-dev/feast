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
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.proto.storage.RedisProto.RedisKeyV2;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
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

    List<RedisKeyV2> redisKeys = buildRedisKeys(project, entityRows);
    List<List<Optional<Feature>>> features = getFeaturesFromRedis(redisKeys, featureReferences);

    return features;
  }

  private List<RedisKeyV2> buildRedisKeys(String project, List<EntityRow> entityRows) {
    List<RedisKeyV2> redisKeys =
        entityRows.stream()
            .map(entityRow -> makeRedisKey(project, entityRow))
            .collect(Collectors.toList());

    return redisKeys;
  }

  /**
   * Create {@link RedisKeyV2}
   *
   * @param project Project where request for features was called from
   * @param entityRow {@link EntityRow}
   * @return {@link RedisKeyV2}
   */
  private RedisKeyV2 makeRedisKey(String project, EntityRow entityRow) {
    RedisKeyV2.Builder builder = RedisKeyV2.newBuilder().setProject(project);
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
              byte[] featureTableTsBytes = getTimestampRedisHashKeyBytes(featureReference);
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
                retrieveFeature(redisValuesList, isTimestampMap, byteToFeatureReferenceMap);
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

  /**
   * Converts all retrieved Redis Hash values based on EntityRows into {@link Feature}
   *
   * @param redisHashValues retrieved Redis Hash values based on EntityRows
   * @param isTimestampMap map to determine if Redis Hash key is a timestamp field
   * @param byteToFeatureReferenceMap map to decode bytes back to FeatureReference
   * @return List of {@link Feature}
   * @throws InvalidProtocolBufferException
   */
  private List<Optional<Feature>> retrieveFeature(
      List<KeyValue<byte[], byte[]>> redisHashValues,
      Map<String, Boolean> isTimestampMap,
      Map<String, FeatureReferenceV2> byteToFeatureReferenceMap)
      throws InvalidProtocolBufferException {
    List<Optional<Feature>> allFeatures = new ArrayList<>();
    Map<FeatureReferenceV2, Optional<Feature.Builder>> allFeaturesBuilderMap = new HashMap<>();
    Map<String, Timestamp> featureTableTimestampMap = new HashMap<>();

    for (int i = 0; i < redisHashValues.size(); i++) {
      if (redisHashValues.get(i).hasValue()) {
        byte[] redisValueK = redisHashValues.get(i).getKey();
        byte[] redisValueV = redisHashValues.get(i).getValue();

        // Decode data from Redis into Feature object fields
        if (isTimestampMap.get(Arrays.toString(redisValueK))) {
          Timestamp eventTimestamp = Timestamp.parseFrom(redisValueV);
          featureTableTimestampMap.put(Arrays.toString(redisValueK), eventTimestamp);
        } else {
          FeatureReferenceV2 featureReference =
              byteToFeatureReferenceMap.get(redisValueK.toString());
          ValueProto.Value featureValue = ValueProto.Value.parseFrom(redisValueV);

          Feature.Builder featureBuilder =
              Feature.builder().setFeatureReference(featureReference).setFeatureValue(featureValue);
          allFeaturesBuilderMap.put(featureReference, Optional.of(featureBuilder));
        }
      }
    }

    // Add timestamp to features
    if (allFeaturesBuilderMap.size() > 0) {
      for (Map.Entry<FeatureReferenceV2, Optional<Feature.Builder>> entry :
          allFeaturesBuilderMap.entrySet()) {
        byte[] timestampFeatureTableHashKeyBytes = getTimestampRedisHashKeyBytes(entry.getKey());
        Timestamp curFeatureTimestamp =
            featureTableTimestampMap.get(Arrays.toString(timestampFeatureTableHashKeyBytes));
        Feature curFeature = entry.getValue().get().setEventTimestamp(curFeatureTimestamp).build();
        allFeatures.add(Optional.of(curFeature));
      }
    }
    return allFeatures;
  }

  private byte[] getTimestampRedisHashKeyBytes(FeatureReferenceV2 featureReference) {
    String timestampRedisHashKeyStr = timestampPrefix + ":" + featureReference.getFeatureTable();
    return timestampRedisHashKeyStr.getBytes();
  }
}
