/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisServingService extends OnlineServingService<RedisKey, byte[]> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisServingService.class);
  private final JedisPool jedisPool;
  private final Tracer tracer;

  public RedisServingService(JedisPool jedisPool, CachedSpecService specService, Tracer tracer) {
    super(specService, tracer);
    this.jedisPool = jedisPool;
    this.tracer = tracer;
  }

  /**
   * Build the redis keys for retrieval from the store.
   *
   * @param featureSetEntityNames entity names that actually belong to the featureSet
   * @param entityRows entity values to retrieve for
   * @param featureSetRequest details of the requested featureSet
   * @return list of RedisKeys
   */
  @Override
  List<RedisKey> createLookupKeys(
      List<String> featureSetEntityNames,
      List<EntityRow> entityRows,
      FeatureSetRequest featureSetRequest) {
    try (Scope scope = tracer.buildSpan("Redis-makeRedisKeys").startActive(true)) {
      String featureSetId =
          String.format("%s:%s", featureSetRequest.getName(), featureSetRequest.getVersion());
      List<RedisKey> redisKeys =
          entityRows.stream()
              .map(row -> makeRedisKey(featureSetId, featureSetEntityNames, row))
              .collect(Collectors.toList());
      return redisKeys;
    }
  }

  @Override
  protected boolean isEmpty(byte[] response) {
    return response == null;
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of {@link RedisKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link RedisKey}
   */
  @Override
  protected List<byte[]> getAll(List<RedisKey> keys) {
    Jedis jedis = jedisPool.getResource();
    byte[][] binaryKeys =
        keys.stream()
            .map(AbstractMessageLite::toByteArray)
            .collect(Collectors.toList())
            .toArray(new byte[0][0]);
    return jedis.mget(binaryKeys);
  }

  @Override
  FeatureRow parseResponse(byte[] response) throws InvalidProtocolBufferException {
    return FeatureRow.parseFrom(response);
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
}
