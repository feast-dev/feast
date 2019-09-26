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

package feast.serving.service.serving;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FeatureDataset;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.service.spec.SpecService;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRow.Builder;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
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
  public GetFeastServingTypeResponse getFeastServingType() {
//    return GetFeastServingTypeResponse.newBuilder().setType().build();
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("Redis-getOnlineFeatures").startActive(true)) {
      List<String> entityNames = request.getEntityDataset().getEntityNamesList();
      List<EntityDatasetRow> entityDataSetRows = request.getEntityDataset()
          .getEntityDatasetRowsList();
      GetOnlineFeaturesResponse.Builder getOnlineFeatureResponseBuilder = GetOnlineFeaturesResponse
          .newBuilder();

      List<FeatureSet> featureSetRequests = request.getFeatureSetsList();
      for (FeatureSet featureSetRequest : featureSetRequests) {
        String featureSetId = String
            .format("%s:%s", featureSetRequest.getName(), featureSetRequest.getVersion());
        FeatureSetSpec featureSetSpec = specService.getFeatureSetSpec(featureSetId);

        Duration defaultMaxAge = featureSetSpec.getMaxAge();
        if (featureSetRequest.getMaxAge() == Duration.getDefaultInstance()) {
          featureSetRequest = featureSetRequest.toBuilder()
              .setMaxAge(defaultMaxAge)
              .build();
        }

        List<RedisKey> redisKeys = getRedisKeys(entityNames, entityDataSetRows, featureSetRequest);
        List<Timestamp> timestamps = entityDataSetRows.stream()
            .map(EntityDatasetRow::getEntityTimestamp).collect(
                Collectors.toList());

        Map<String, Field> fields = new LinkedHashMap<>();
        featureSetRequest.getFeatureNamesList().stream()
            .forEach(name -> fields.put(name, Field.newBuilder().setName(name).build()));

        List<FeatureRow> featureRows = new ArrayList<>();
        try {
          featureRows = sendAndProcessMultiGet(redisKeys, timestamps, fields,
              featureSetRequest);
        } catch (InvalidProtocolBufferException e) {
          throw new FeatureRetrievalException("Unable to parse protobuf while retrieving feature",
              e);
        } finally {
          FeatureDataset featureDataSet = FeatureDataset.newBuilder()
              .setName(featureSetRequest.getName())
              .setVersion(featureSetRequest.getVersion())
              .addAllFeatureRows(featureRows)
              .build();

          getOnlineFeatureResponseBuilder.addFeatureDatasets(featureDataSet);
        }
      }
      return getOnlineFeatureResponseBuilder.build();
    }
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest request) {
    // Not implemented
    return null;
  }

  @Override
  public GetBatchFeaturesFromCompletedJobResponse getBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest request) {
    return null;
  }

  @Override
  public GetStagingLocationResponse getStagingLocation(GetStagingLocationRequest request) {
    return null;
  }

  @Override
  public LoadBatchFeaturesResponse loadBatchFeatures(LoadBatchFeaturesRequest request) {
    return null;
  }

  @Override
  public ReloadJobStatusResponse reloadJobStatus(ReloadJobStatusRequest request) {
    return null;
  }

  /**
   * Build the redis keys for retrieval from the store.
   *
   * @param entityNames column names of the entityDataset
   * @param entityDatasetRows entity values to retrieve for
   * @param featureSetRequest details of the requested featureSet
   * @return list of RedisKeys
   */
  private List<RedisKey> getRedisKeys(List<String> entityNames,
      List<EntityDatasetRow> entityDatasetRows, FeatureSet featureSetRequest) {
    try (Scope scope = tracer.buildSpan("Redis-makeRedisKeys").startActive(true)) {
      String featureSetId = String.format("%s:%s", featureSetRequest.getName(), featureSetRequest.getVersion());
      List<RedisKey> redisKeys = entityDatasetRows.parallelStream()
          .map(row -> makeRedisKey(featureSetId, entityNames, row))
          .collect(Collectors.toList());
      return redisKeys;
    }
  }

  /**
   * Create a list of {@link FeatureRow}
   *
   * @param redisKeys list of {@link RedisKey} to be retrieved from Redis
   * @param fields map of featureId to corresponding empty field
   * @param featureSetRequest {@link FeatureSet} so that featureSetName and featureSerVersion can be
   * retrieved
   * @return list of {@link FeatureRow}
   * @throws InvalidProtocolBufferException Exception that is thrown the FeatureRow cannot be parsed
   * from the byte array response
   */
  private List<FeatureRow> sendAndProcessMultiGet(List<RedisKey> redisKeys,
      List<Timestamp> timestamps,
      Map<String, Field> fields, FeatureSet featureSetRequest)
      throws InvalidProtocolBufferException {
    List<byte[]> jedisResps = sendMultiGet(redisKeys);

    try (Scope scope = tracer.buildSpan("Redis-processResponse").startActive(true)) {
      List<FeatureRow> featureRows = new ArrayList<>();

      for (int i = 0; i < jedisResps.size(); i++) {
        featureRows.add(
            buildFeatureRow(jedisResps.get(i), featureSetRequest,
                redisKeys.get(i).getEntitiesList(), timestamps.get(i), fields));
      }
      return featureRows;
    }
  }


  /**
   * Build the featureRow based on the request and the response from redis.
   * In the case of the following, empty featureRows will be returned:
   *
   * 1. Redis returns null, the key provided does not exist in the store <p></p>
   * 2. The key stored in the store exceeds the maximum age specified in the request
   *
   * Otherwise, a featureRow will be built, excluding any columns not specified by
   * the user. If any columns are missing in the redis featureRow, the field will still
   * be present in the final featureRow, but the value will be left unset.
   *
   * @param jedisResponse response from redis, in bytes
   * @param featureSetRequest details about the requested featureSet
   * @param entities list of entity fields, which will be appended to the featureRow
   * @param timestamp timestamp of the request, will be used to calculate age of the response
   * @param fields map of featureId to corresponding empty field
   * @return FeatureRow containing the entity and requested feature fields
   * @throws InvalidProtocolBufferException
   */
  private FeatureRow buildFeatureRow(byte[] jedisResponse,
      FeatureSet featureSetRequest, List<Field> entities,
      Timestamp timestamp, Map<String, Field> fields) throws InvalidProtocolBufferException {
    Builder featureRowBuilder = FeatureRow.newBuilder()
        .setFeatureSet(
            String.format("%s:%s", featureSetRequest.getName(), featureSetRequest.getVersion()))
        .addAllFields(entities)
        .setEventTimestamp(Timestamp.newBuilder());

    if (jedisResponse == null) {
      return featureRowBuilder
          .addAllFields(fields.values())
          .build();
    }

    Map<String, Field> fieldsCopy = new LinkedHashMap<>(fields);
    FeatureRow featureRow = FeatureRow.parseFrom(jedisResponse);

    long timeDifference = timestamp.getSeconds() - featureRow.getEventTimestamp().getSeconds();
    if (timeDifference > featureSetRequest.getMaxAge().getSeconds()) {
      return featureRowBuilder
          .addAllFields(fields.values())
          .build();
    }

    featureRow.getFieldsList().stream()
        .filter(f -> fields.keySet().contains(f.getName()))
        .forEach(f -> fieldsCopy.put(f.getName(), f));

    return featureRowBuilder
        .addAllFields(fieldsCopy.values())
        .setEventTimestamp(featureRow.getEventTimestamp())
        .build();
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
        throw new FeatureRetrievalException("Unable to retrieve feature from Redis", e);
      }
    }
  }

  /**
   * Create {@link RedisKey}
   *
   * @param featureSet featureSet reference of the feature. E.g. feature_set_1:1
   * @param entityNames list of entityName
   * @param entityDatasetRow entityDataSetRow to build the key from
   * @return {@link RedisKey}
   */
  private RedisKey makeRedisKey(String featureSet, List<String> entityNames,
      EntityDatasetRow entityDatasetRow) {
    RedisKey.Builder builder = RedisKey.newBuilder().setFeatureSet(featureSet);
    for (int i = 0; i < entityNames.size(); i++) {
      String entityName = entityNames.get(i);
      Value entityVal = entityDatasetRow.getEntityIds(i);
      builder.addEntities(Field.newBuilder().setName(entityName).setValue(entityVal));
    }
    return builder.build();
  }

}
