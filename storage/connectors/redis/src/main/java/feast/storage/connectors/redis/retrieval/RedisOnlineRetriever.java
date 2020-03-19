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
package feast.storage.connectors.redis.retrieval;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.storage.RedisProto.RedisKey;
import feast.storage.api.retrieval.FeatureSetRequest;
import feast.storage.api.retrieval.OnlineRetriever;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.grpc.Status;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class RedisOnlineRetriever implements OnlineRetriever {

  private final RedisCommands<byte[], byte[]> syncCommands;

  public RedisOnlineRetriever(StatefulRedisConnection<byte[], byte[]> connection) {
    this.syncCommands = connection.sync();
  }

  /**
   * Gets online features from redis. This method returns a list of {@link FeatureRow}s
   * corresponding to each feature set spec. Each feature row in the list then corresponds to an
   * {@link EntityRow} provided by the user.
   *
   * @param entityRows list of entity rows in the feature request
   * @param featureSetRequests Map of {@link feast.core.FeatureSetProto.FeatureSetSpec} to feature
   *     references in the request tied to that feature set.
   * @return List of List of {@link FeatureRow}
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
   * @param featureSet featureSet reference of the feature. E.g. feature_set_1:1
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
   * Send a list of get request as an mget
   *
   * @param keys list of {@link RedisKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link RedisKey}
   */
  private List<byte[]> sendMultiGet(List<RedisKey> keys) {
    try {
      byte[][] binaryKeys =
          keys.stream()
              .map(AbstractMessageLite::toByteArray)
              .collect(Collectors.toList())
              .toArray(new byte[0][0]);
      return syncCommands.mget(binaryKeys).stream()
          .map(keyValue -> keyValue.getValueOrElse(null))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw Status.NOT_FOUND
          .withDescription("Unable to retrieve feature from Redis")
          .withCause(e)
          .asRuntimeException();
    }
  }

  // TODO: Refactor this out to common package?
  private static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    String ref = String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
    if (featureSetSpec.getVersion() > 0) {
      return ref + String.format(":%d", featureSetSpec.getVersion());
    }
    return ref;
  }
}
