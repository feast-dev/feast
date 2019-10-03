/*
 * Copyright 2019 The Feast Authors
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

import com.google.api.client.util.Lists;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.serving.ServingAPIProto.ReloadJobRequest;
import feast.serving.ServingAPIProto.ReloadJobResponse;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Slf4j
public class RedisServingService implements ServingService {

  private final JedisPool jedisPool;
  private final SpecService specService;
  private final Tracer tracer;

  public RedisServingService(JedisPool jedisPool, SpecService specService, Tracer tracer) {
    this.jedisPool = jedisPool;
    this.specService = specService;
    this.tracer = tracer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetFeastServingTypeResponse getFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest) {
    return GetFeastServingTypeResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("Redis-getOnlineFeatures").startActive(true)) {
      GetOnlineFeaturesResponse.Builder getOnlineFeaturesResponseBuilder = GetOnlineFeaturesResponse
          .newBuilder();

      List<EntityRow> entityRows = request.getEntityRowsList();
      Map<EntityRow, Map<String, Value>> featureValuesMap =
          entityRows.stream()
              .collect(Collectors.toMap(er -> er, er -> Maps.newHashMap(er.getFieldsMap())));

      List<FeatureSet> featureSetRequests = request.getFeatureSetsList();
      for (FeatureSet featureSetRequest : featureSetRequests) {

        GetFeatureSetsRequest getFeatureSetSpecRequest =
            GetFeatureSetsRequest.newBuilder()
                .setFilter(
                    Filter.newBuilder()
                        .setFeatureSetName(featureSetRequest.getName())
                        .setFeatureSetVersion(String.valueOf(featureSetRequest.getVersion()))
                        .build())
                .build();

        FeatureSetSpec featureSetSpec =
            specService.getFeatureSets(getFeatureSetSpecRequest).getFeatureSets(0);

        List<String> featureSetEntityNames =
            featureSetSpec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());

        Duration defaultMaxAge = featureSetSpec.getMaxAge();
        if (featureSetRequest.getMaxAge() == Duration.getDefaultInstance()) {
          featureSetRequest = featureSetRequest.toBuilder().setMaxAge(defaultMaxAge).build();
        }

        List<RedisKey> redisKeys = getRedisKeys(featureSetEntityNames, entityRows,
            featureSetRequest);

        try {
          sendAndProcessMultiGet(redisKeys, entityRows, featureValuesMap, featureSetRequest);
        } catch (InvalidProtocolBufferException e) {
          throw Status.INTERNAL
              .withDescription("Unable to parse protobuf while retrieving feature")
              .withCause(e)
              .asRuntimeException();
        } finally {
          List<FieldValues> fieldValues = featureValuesMap.values().stream()
              .map(m -> FieldValues.newBuilder().putAllFields(m).build())
              .collect(Collectors.toList());
          return getOnlineFeaturesResponseBuilder.addAllFieldValues(fieldValues).build();
        }
      }
      return getOnlineFeaturesResponseBuilder.build();
    }
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }


  @Override
  public ReloadJobResponse reloadJob(ReloadJobRequest reloadJobRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  /**
   * Build the redis keys for retrieval from the store.
   *
   * @param featureSetEntityNames entity names that actually belong to the featureSet
   * @param entityRows entity values to retrieve for
   * @param featureSetRequest details of the requested featureSet
   * @return list of RedisKeys
   */
  private List<RedisKey> getRedisKeys(
      List<String> featureSetEntityNames,
      List<EntityRow> entityRows,
      FeatureSet featureSetRequest) {
    try (Scope scope = tracer.buildSpan("Redis-makeRedisKeys").startActive(true)) {
      String featureSetId =
          String.format("%s:%s", featureSetRequest.getName(), featureSetRequest.getVersion());
      List<RedisKey> redisKeys =
          entityRows
              .stream()
              .map(row -> makeRedisKey(featureSetId, featureSetEntityNames, row))
              .collect(Collectors.toList());
      return redisKeys;
    }
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
      String featureSet,
      List<String> featureSetEntityNames,
      EntityRow entityRow) {
    RedisKey.Builder builder = RedisKey.newBuilder().setFeatureSet(featureSet);
    Map<String, Value> fieldsMap = entityRow.getFieldsMap();
    for (int i = 0; i < featureSetEntityNames.size(); i++) {
      String entityName = featureSetEntityNames.get(i);
      builder.addEntities(Field.newBuilder()
          .setName(entityName)
          .setValue(fieldsMap.get(entityName)));
    }
    return builder.build();
  }

  private void sendAndProcessMultiGet(
      List<RedisKey> redisKeys,
      List<EntityRow> entityRows,
      Map<EntityRow, Map<String, Value>> featureValuesMap,
      FeatureSet featureSetRequest)
      throws InvalidProtocolBufferException {

    List<byte[]> jedisResps = sendMultiGet(redisKeys);

    try (Scope scope = tracer.buildSpan("Redis-processResponse").startActive(true)) {
      String featureSetId = String
          .format("%s:%d", featureSetRequest.getName(), featureSetRequest.getVersion());
      Map<String, Value> nullValues = featureSetRequest.getFeatureNamesList().stream()
          .collect(Collectors
              .toMap(name -> featureSetId + "." + name, name -> Value.newBuilder().build()));
      for (int i = 0; i < jedisResps.size(); i++) {
        EntityRow entityRow = entityRows.get(i);
        Map<String, Value> featureValues = featureValuesMap.get(entityRow);
        byte[] jedisResponse = jedisResps.get(i);
        if (jedisResponse == null) {
          featureValues.putAll(nullValues);
          continue;
        }
        FeatureRow featureRow = FeatureRow.parseFrom(jedisResponse);
        long givenTimestamp = entityRow.getEntityTimestamp().getSeconds();
        if (givenTimestamp == 0) {
          givenTimestamp = System.currentTimeMillis();
        }
        long timeDifference =
            givenTimestamp - featureRow.getEventTimestamp()
                .getSeconds();
        if (timeDifference > featureSetRequest.getMaxAge().getSeconds()) {
          featureValues.putAll(nullValues);
          continue;
        }
        featureRow.getFieldsList().stream()
            .filter(f -> featureSetRequest.getFeatureNamesList().contains(f.getName()))
            .forEach(f -> featureValues.put(featureSetId + "." + f.getName(), f.getValue()));
      }
    }
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of {@link RedisKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link RedisKey}
   */
  private List<byte[]> sendMultiGet(List<RedisKey> keys) {
    try (Scope scope = tracer.buildSpan("Redis-sendMultiGet").startActive(true)) {
      try (Jedis jedis = jedisPool.getResource()) {
        byte[][] binaryKeys =
            keys.stream()
                .map(AbstractMessageLite::toByteArray)
                .collect(Collectors.toList())
                .toArray(new byte[0][0]);
        return jedis.mget(binaryKeys);
      } catch (Exception e) {
        throw Status.NOT_FOUND
            .withDescription("Unable to retrieve feature from Redis")
            .withCause(e)
            .asRuntimeException();
      }
    }
  }
}
