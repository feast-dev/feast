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

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import io.grpc.Status;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class RedisOnlineRetriever implements OnlineRetriever {

  private final RedisCommands<byte[], byte[]> syncCommands;

  private RedisOnlineRetriever(StatefulRedisConnection<byte[], byte[]> connection) {
    this.syncCommands = connection.sync();
  }

  public static OnlineRetriever create(Map<String, String> config) {

    StatefulRedisConnection<byte[], byte[]> connection =
        RedisClient.create(
                RedisURI.create(config.get("host"), Integer.parseInt(config.get("port"))))
            .connect(new ByteArrayCodec());

    return new RedisOnlineRetriever(connection);
  }

  public static OnlineRetriever create(StatefulRedisConnection<byte[], byte[]> connection) {
    return new RedisOnlineRetriever(connection);
  }

  /** {@inheritDoc} */
  @Override
  public List<Optional<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, FeatureSetRequest featureSetRequest) {

    // get features for this features/featureset in featureset request
    FeatureSetSpec featureSetSpec = featureSetRequest.getSpec();
    List<RedisKey> redisKeys = buildRedisKeys(entityRows, featureSetSpec);
    FeatureRowDecoder decoder =
        new FeatureRowDecoder(generateFeatureSetStringRef(featureSetSpec), featureSetSpec);
    List<Optional<FeatureRow>> featureRows = new ArrayList<>();
    try {
      featureRows = getFeaturesFromRedis(redisKeys, decoder);
    } catch (InvalidProtocolBufferException | ExecutionException e) {
      throw Status.INTERNAL
          .withDescription("Unable to parse protobuf while retrieving feature")
          .withCause(e)
          .asRuntimeException();
    }

    return featureRows;
  }

  private List<RedisKey> buildRedisKeys(List<EntityRow> entityRows, FeatureSetSpec featureSetSpec) {
    String featureSetRef = generateFeatureSetStringRef(featureSetSpec);
    List<String> featureSetEntityNames =
        featureSetSpec.getEntitiesList().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toList());
    List<RedisKey> redisKeys =
        entityRows.stream()
            .map(row -> makeRedisKey(featureSetRef, featureSetEntityNames, row))
            .collect(Collectors.toList());
    return redisKeys;
  }

  /**
   * Create {@link RedisKey}
   *
   * @param featureSet featureSet reference of the feature. E.g. feature_set_1
   * @param featureSetEntityNames entity names that belong to the featureSet
   * @param entityRow entityRow to build the key from
   * @return {@link RedisKey}
   */
  private RedisKey makeRedisKey(
      String featureSet, List<String> featureSetEntityNames, EntityRow entityRow) {
    RedisKey.Builder builder = RedisKey.newBuilder().setFeatureSet(featureSet);
    Map<String, Value> fieldsMap = entityRow.getFieldsMap();
    featureSetEntityNames.sort(String::compareTo);
    for (int i = 0; i < featureSetEntityNames.size(); i++) {
      String entityName = featureSetEntityNames.get(i);

      if (!fieldsMap.containsKey(entityName)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Entity row fields \"%s\" does not contain required entity field \"%s\"",
                    fieldsMap.keySet().toString(), entityName))
            .asRuntimeException();
      }

      builder.addEntities(
          Field.newBuilder().setName(entityName).setValue(fieldsMap.get(entityName)));
    }
    return builder.build();
  }

  /**
   * Get features from data pulled from the Redis for a specific featureset.
   *
   * @param redisKeys keys used to retrieve data from Redis for a specific featureset.
   * @param decoder used to decode the data retrieved from Redis for a specific featureset.
   * @return List of {@link FeatureRow} optionals
   */
  private List<Optional<FeatureRow>> getFeaturesFromRedis(
      List<RedisKey> redisKeys, FeatureRowDecoder decoder)
      throws InvalidProtocolBufferException, ExecutionException {
    // pull feature row data bytes from redis using given redis keys
    List<byte[]> featureRowsBytes = sendMultiGet(redisKeys);
    List<Optional<FeatureRow>> featureRows = new ArrayList<>();

    for (byte[] featureRowBytes : featureRowsBytes) {
      if (featureRowBytes == null) {
        featureRows.add(Optional.empty());
        continue;
      }

      // decode feature rows from data bytes using decoder.
      FeatureRow featureRow = FeatureRow.parseFrom(featureRowBytes);
      if (decoder.isEncoded(featureRow) && decoder.isEncodingValid(featureRow)) {
        featureRow = decoder.decode(featureRow);
      } else {
        // decoding feature row failed: data corruption could have occurred
        throw Status.DATA_LOSS
            .withDescription(
                "Failed to decode FeatureRow from bytes retrieved from redis"
                    + ": Possible data corruption")
            .asRuntimeException();
      }
      featureRows.add(Optional.of(featureRow));
    }
    return featureRows;
  }

  /**
   * Pull the data stored in Redis at the given keys as bytes using the mget command. If no data is
   * stored at a given key in Redis, will subsitute the data with null.
   *
   * @param keys list of {@link RedisKey} to pull from redis.
   * @return list of data bytes or null pulled from redis for each given key.
   */
  private List<byte[]> sendMultiGet(List<RedisKey> keys) {
    try {
      byte[][] binaryKeys =
          keys.stream()
              .map(AbstractMessageLite::toByteArray)
              .collect(Collectors.toList())
              .toArray(new byte[0][0]);
      return syncCommands.mget(binaryKeys).stream()
          .map(
              keyValue -> {
                if (keyValue == null) {
                  return null;
                }
                return keyValue.getValueOrElse(null);
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw Status.UNKNOWN
          .withDescription("Unexpected error when pulling data from from Redis.")
          .withCause(e)
          .asRuntimeException();
    }
  }

  // TODO: Refactor this out to common package
  private static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    String ref = String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
    return ref;
  }
}
