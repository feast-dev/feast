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

import com.google.common.hash.Hashing;
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
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class RedisOnlineRetrieverV2 implements OnlineRetrieverV2 {

  private static final String timestampPrefix = "_ts";
  private final RedisCommands<byte[], byte[]> syncCommands;

  private RedisOnlineRetrieverV2(StatefulRedisConnection<byte[], byte[]> connection) {
    this.syncCommands = connection.sync();
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

    try {
      List<List<byte[]>> featureReferenceWithTsByteArrays = new ArrayList<>();
      featureReferences.stream()
          .forEach(
              featureReference -> {
                List<byte[]> currentFeatureWithTsByteArrays = new ArrayList<>();

                // eg. murmur(<featuretable_name:feature_name>)
                String delimitedFeatureReference =
                    featureReference.getFeatureTable() + ":" + featureReference.getName();
                byte[] featureReferenceBytes =
                    Hashing.murmur3_32()
                        .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
                        .asBytes();
                currentFeatureWithTsByteArrays.add(featureReferenceBytes);
                isTimestampMap.put(Arrays.toString(featureReferenceBytes), false);
                byteToFeatureReferenceMap.put(featureReferenceBytes.toString(), featureReference);

                // eg. <_ts:featuretable_name>
                String timestampFeatureTableReference =
                    timestampPrefix + ":" + featureReference.getFeatureTable();
                byte[] featureTableTsBytes = timestampFeatureTableReference.getBytes();
                isTimestampMap.put(Arrays.toString(featureTableTsBytes), true);
                currentFeatureWithTsByteArrays.add(featureTableTsBytes);

                featureReferenceWithTsByteArrays.add(currentFeatureWithTsByteArrays);
              });

      // Access redis keys and extract features
      for (byte[] binaryRedisKey : binaryRedisKeys) {
        List<Optional<Feature>> curRedisKeyFeatures = new ArrayList<>();
        // Loop according to each FeatureReferenceV2 bytes
        for (List<byte[]> currentFeatureReferenceWithTsByteArray :
            featureReferenceWithTsByteArrays) {
          FeatureReferenceV2 featureReference = null;
          ValueProto.Value featureValue = null;
          Timestamp eventTimestamp = null;

          // Always 2 fields (i.e feature and timestamp)
          List<KeyValue<byte[], byte[]>> redisValuesList =
              syncCommands.hmget(
                  binaryRedisKey,
                  currentFeatureReferenceWithTsByteArray.get(0),
                  currentFeatureReferenceWithTsByteArray.get(1));

          for (int i = 0; i < redisValuesList.size(); i++) {
            if (redisValuesList.get(i).hasValue()) {
              byte[] redisValueK = redisValuesList.get(i).getKey();
              byte[] redisValueV = redisValuesList.get(i).getValue();

              // Decode data from Redis into Feature object fields
              if (isTimestampMap.get(Arrays.toString(redisValueK))) {
                redisValueV = redisValuesList.get(i).getValueOrElse(new byte[0]);
                eventTimestamp = Timestamp.parseFrom(redisValueV);
              } else {
                featureReference = byteToFeatureReferenceMap.get(redisValueK.toString());
                featureValue = ValueProto.Value.parseFrom(redisValueV);
              }
            }
          }
          // Check for null featureReference i.e key is not found
          if (featureReference != null) {
            Feature feature =
                Feature.builder()
                    .setFeatureReference(featureReference)
                    .setFeatureValue(featureValue)
                    .setEventTimestamp(eventTimestamp)
                    .build();
            curRedisKeyFeatures.add(Optional.of(feature));
          }
        }
        features.add(curRedisKeyFeatures);
      }
    } catch (Exception e) {
      throw Status.UNKNOWN
          .withDescription("Unexpected error when pulling data from from Redis.")
          .withCause(e)
          .asRuntimeException();
    }
    return features;
  }
}
