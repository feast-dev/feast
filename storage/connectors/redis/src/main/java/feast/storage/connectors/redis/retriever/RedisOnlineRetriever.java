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
import feast.proto.serving.ServingAPIProto.FeatureReference;
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

  /**
   * Gets online features from redis store for the given entity rows using data retrieved from the
   * feature/featureset specified in feature set requests.
   *
   * <p>This method returns a list of {@link FeatureRow}s corresponding to each feature set spec.
   * Each feature row in the list then corresponds to an {@link EntityRow} provided by the user. If
   * retrieval fails for a given entity row, will return null in place of the {@link FeatureRow}.
   *
   * @param featureSetRequests List of {@link FeatureSetRequest} specifying the features/feature set
   *     to retrieve data from.
   * @return list of lists of {@link FeatureRow}s corresponding to data retrieved for each feature
   *     set request and entity row.
   */
  @Override
  public List<List<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, List<FeatureSetRequest> featureSetRequests) {

    List<List<FeatureRow>> featureRows = new ArrayList<>();
    for (FeatureSetRequest featureSetRequest : featureSetRequests) {
      List<RedisKey> redisKeys = buildRedisKeys(entityRows, featureSetRequest.getSpec());
      try {
        List<FeatureRow> featureRowsForFeatureSet =
            sendAndProcessMultiGet(
                redisKeys,
                featureSetRequest.getSpec(),
                featureSetRequest.getFeatureReferences().asList());
        featureRows.add(featureRowsForFeatureSet);
      } catch (InvalidProtocolBufferException | ExecutionException e) {
        throw Status.INTERNAL
            .withDescription("Unable to parse protobuf while retrieving feature")
            .withCause(e)
            .asRuntimeException();
      }
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

  private List<FeatureRow> sendAndProcessMultiGet(
      List<RedisKey> redisKeys,
      FeatureSetSpec featureSetSpec,
      List<FeatureReference> featureReferences)
      throws InvalidProtocolBufferException, ExecutionException {

    List<byte[]> values = sendMultiGet(redisKeys);
    List<FeatureRow> featureRows = new ArrayList<>();

    FeatureRow.Builder nullFeatureRowBuilder =
        FeatureRow.newBuilder().setFeatureSet(generateFeatureSetStringRef(featureSetSpec));
    for (FeatureReference featureReference : featureReferences) {
      nullFeatureRowBuilder.addFields(Field.newBuilder().setName(featureReference.getName()));
    }

    for (int i = 0; i < values.size(); i++) {

      byte[] value = values.get(i);
      if (value == null) {
        featureRows.add(nullFeatureRowBuilder.build());
        continue;
      }

      FeatureRow featureRow = FeatureRow.parseFrom(value);
      String featureSetRef = redisKeys.get(i).getFeatureSet();
      FeatureRowDecoder decoder = new FeatureRowDecoder(featureSetRef, featureSetSpec);
      if (decoder.isEncoded(featureRow)) {
        if (decoder.isEncodingValid(featureRow)) {
          featureRow = decoder.decode(featureRow);
        } else {
          featureRows.add(nullFeatureRowBuilder.build());
          continue;
        }
      }

      featureRows.add(featureRow);
    }
    return featureRows;
  }

  /**
   * Pull the data stored in redis at the given keys as bytes using the mget command. If no data is
   * stored at a given key in redis, will subsitute the data with null.
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

  // TODO: Refactor this out to common package? move to Ref utils
  private static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    String ref = String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
    return ref;
  }
}
