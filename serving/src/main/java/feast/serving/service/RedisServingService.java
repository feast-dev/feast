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

import static feast.serving.util.Metrics.missingKeyCount;
import static feast.serving.util.Metrics.requestCount;
import static feast.serving.util.Metrics.requestLatency;
import static feast.serving.util.Metrics.staleKeyCount;

import com.google.common.collect.Maps;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
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

public class RedisServingService implements ServingService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisServingService.class);
  private final JedisPool jedisPool;
  private final CachedSpecService specService;
  private final Tracer tracer;

  public RedisServingService(JedisPool jedisPool, CachedSpecService specService, Tracer tracer) {
    this.jedisPool = jedisPool;
    this.specService = specService;
    this.tracer = tracer;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("Redis-getOnlineFeatures").startActive(true)) {
      long startTime = System.currentTimeMillis();
      GetOnlineFeaturesResponse.Builder getOnlineFeaturesResponseBuilder =
          GetOnlineFeaturesResponse.newBuilder();

      List<EntityRow> entityRows = request.getEntityRowsList();
      Map<EntityRow, Map<String, Value>> featureValuesMap =
          entityRows.stream()
              .collect(Collectors.toMap(er -> er, er -> Maps.newHashMap(er.getFieldsMap())));

      List<FeatureSetRequest> featureSetRequests = request.getFeatureSetsList();
      for (FeatureSetRequest featureSetRequest : featureSetRequests) {

        FeatureSetSpec featureSetSpec =
            specService.getFeatureSet(featureSetRequest.getName(), featureSetRequest.getVersion());

        List<String> featureSetEntityNames =
            featureSetSpec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());

        Duration defaultMaxAge = featureSetSpec.getMaxAge();
        if (featureSetRequest.getMaxAge().equals(Duration.getDefaultInstance())) {
          featureSetRequest = featureSetRequest.toBuilder().setMaxAge(defaultMaxAge).build();
        }

        List<RedisKey> redisKeys =
            getRedisKeys(featureSetEntityNames, entityRows, featureSetRequest);

        try {
          sendAndProcessMultiGet(redisKeys, entityRows, featureValuesMap, featureSetRequest);
        } catch (InvalidProtocolBufferException e) {
          throw Status.INTERNAL
              .withDescription("Unable to parse protobuf while retrieving feature")
              .withCause(e)
              .asRuntimeException();
        }
      }
      List<FieldValues> fieldValues =
          featureValuesMap.values().stream()
              .map(m -> FieldValues.newBuilder().putAllFields(m).build())
              .collect(Collectors.toList());
      requestLatency.labels("getOnlineFeatures").observe(System.currentTimeMillis() - startTime);
      return getOnlineFeaturesResponseBuilder.addAllFieldValues(fieldValues).build();
    }
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
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

  private void sendAndProcessMultiGet(
      List<RedisKey> redisKeys,
      List<EntityRow> entityRows,
      Map<EntityRow, Map<String, Value>> featureValuesMap,
      FeatureSetRequest featureSetRequest)
      throws InvalidProtocolBufferException {

    List<byte[]> jedisResps = sendMultiGet(redisKeys);
    long startTime = System.currentTimeMillis();
    try (Scope scope = tracer.buildSpan("Redis-processResponse").startActive(true)) {
      String featureSetId =
          String.format("%s:%d", featureSetRequest.getName(), featureSetRequest.getVersion());

      Map<String, Value> nullValues =
          featureSetRequest.getFeatureNamesList().stream()
              .collect(
                  Collectors.toMap(
                      name -> featureSetId + ":" + name, name -> Value.newBuilder().build()));

      for (int i = 0; i < jedisResps.size(); i++) {
        EntityRow entityRow = entityRows.get(i);
        Map<String, Value> featureValues = featureValuesMap.get(entityRow);

        byte[] jedisResponse = jedisResps.get(i);
        if (jedisResponse == null) {
          missingKeyCount.labels(featureSetRequest.getName()).inc();
          featureValues.putAll(nullValues);
          continue;
        }

        FeatureRow featureRow = FeatureRow.parseFrom(jedisResponse);

        boolean stale = isStale(featureSetRequest, entityRow, featureRow);
        if (stale) {
          staleKeyCount.labels(featureSetRequest.getName()).inc();
          featureValues.putAll(nullValues);
          continue;
        }

        requestCount.labels(featureSetRequest.getName()).inc();
        featureRow.getFieldsList().stream()
            .filter(f -> featureSetRequest.getFeatureNamesList().contains(f.getName()))
            .forEach(f -> featureValues.put(featureSetId + ":" + f.getName(), f.getValue()));
      }
    } finally {
      requestLatency.labels("processResponse").observe(System.currentTimeMillis() - startTime);
    }
  }

  private boolean isStale(
      FeatureSetRequest featureSetRequest, EntityRow entityRow, FeatureRow featureRow) {
    if (featureSetRequest.getMaxAge().equals(Duration.getDefaultInstance())) {
      return false;
    }
    long givenTimestamp = entityRow.getEntityTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - featureRow.getEventTimestamp().getSeconds();
    return timeDifference > featureSetRequest.getMaxAge().getSeconds();
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of {@link RedisKey}
   * @return list of {@link FeatureRow} in primitive byte representation for each {@link RedisKey}
   */
  private List<byte[]> sendMultiGet(List<RedisKey> keys) {
    try (Scope scope = tracer.buildSpan("Redis-sendMultiGet").startActive(true)) {
      long startTime = System.currentTimeMillis();
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
      } finally {
        requestLatency.labels("sendMultiGet").observe(System.currentTimeMillis() - startTime);
      }
    }
  }
}
