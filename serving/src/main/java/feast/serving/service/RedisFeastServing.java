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

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusResponse;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlResponse;
import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FeatureDataSet;
import feast.serving.exception.FeatureRetrievalException;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureProto.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Slf4j
public class RedisFeastServing implements FeastServing {

  private final JedisPool jedisPool;
  private final Tracer tracer;

  public RedisFeastServing(JedisPool jedisPool, Tracer tracer) {
    this.jedisPool = jedisPool;
    this.tracer = tracer;
  }

  @Override
  public GetFeastServingVersionResponse getFeastServingVersion() {
    String artifactVersion = this.getClass().getPackage().getImplementationVersion();
    return GetFeastServingVersionResponse.newBuilder().setVersion(artifactVersion).build();
  }

  @Override
  public GetFeastServingTypeResponse getFeastServingType() {
//    return GetFeastServingTypeResponse.newBuilder().setType().build();
    return null;
  }

  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("Redis-getOnlineFeatures").startActive(true)) {
      List<String> entityNames = request.getEntityDataSet().getFieldNamesList();
      List<EntityDataSetRow> entityDataSetRows = request.getEntityDataSet()
          .getEntityDataSetRowsList();
      GetOnlineFeaturesResponse.Builder getOnlineFeatureResponseBuilder = GetOnlineFeaturesResponse
          .newBuilder();

      // Create a list of keys to be fetched from Redis
      List<FeatureSet> featureSets = request.getFeatureSetsList();
      for (FeatureSet featureSet : featureSets) {
        List<RedisKey> redisKeys = new ArrayList<>();
        String featureSetId = String.format("%s:%s", featureSet.getName(), featureSet.getVersion());
        for (EntityDataSetRow entityDataSetRow : entityDataSetRows) {
          redisKeys.add(makeRedisKey(featureSetId, entityNames, entityDataSetRow));
        }

        // Convert ProtocolStringList to list of Strings
        List<String> requestedColumns = featureSet.getFeatureNamesList()
            .asByteStringList().stream()
            .map(ByteString::toStringUtf8)
            .collect(Collectors.toList());
        requestedColumns.addAll(entityNames);
        requestedColumns.add("datetime");

        List<FeatureRow> featureRows = new ArrayList<>();
        try {
          featureRows = sendAndProcessMultiGet(redisKeys, requestedColumns, featureSet);
        } catch (NullPointerException e) {
          log.error("No keys matching {} found in store", redisKeys);
        } catch (InvalidProtocolBufferException e) {
          log.error("Unable to parse protobuf", e);
          throw new FeatureRetrievalException("Unable to parse protobuf while retrieving feature",
              e);
        } finally {
          FeatureDataSet featureDataSet = FeatureDataSet.newBuilder().setName(featureSet.getName())
              .setVersion(featureSet.getVersion()).addAllFeatureRows(featureRows).build();
          getOnlineFeatureResponseBuilder.addFeatureDataSets(featureDataSet);
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
  public GetStatusResponse getBatchFeaturesJobStatus(GetStatusRequest request) {
    // Not implemented
    return null;
  }

  @Override
  public GetUploadUrlResponse getBatchFeaturesJobUploadUrl(GetUploadUrlRequest request) {
    // Not implemented
    return null;
  }

  @Override
  public SetUploadCompleteResponse setBatchFeaturesJobUploadComplete(
      SetUploadCompleteRequest request) {
    // Not implemented
    return null;
  }

  private List<FeatureRow> sendAndProcessMultiGet(List<RedisKey> redisKeys,
      List<String> requestedColumns, FeatureSet featureSet) throws InvalidProtocolBufferException {
    List<byte[]> jedisResps = sendMultiGet(redisKeys);

    List<FeatureRow> featureRows = new ArrayList<>();
    for (byte[] jedisResp : jedisResps) {
      FeatureRow featureRow = FeatureRow.parseFrom(jedisResp);
      List<Field> fields = featureRow.getFieldsList().stream()
          .filter(f -> requestedColumns.contains(f.getName())).collect(Collectors.toList());
      featureRows.add(FeatureRow.newBuilder().addAllFields(fields)
          .setEventTimestamp(featureRow.getEventTimestamp())
          .setFeatureSet(String.format("%s:%s", featureSet.getName(), featureSet.getVersion()))
          .build());
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
    try (Scope scope = tracer.buildSpan("Redis-sendMultiGet").startActive(true)) {
      try (Jedis jedis = jedisPool.getResource()) {
        byte[][] binaryKeys =
            keys.stream()
                .map(AbstractMessageLite::toByteArray)
                .collect(Collectors.toList())
                .toArray(new byte[0][0]);
        return jedis.mget(binaryKeys);
      } catch (Exception e) {
        log.error("Exception while retrieving feature from Redis", e);
        throw new FeatureRetrievalException("Unable to retrieve feature from Redis", e);
      }
    }
  }

  /**
   * Create {@link RedisKey}
   *
   * @param featureSet featureSet reference of the feature. E.g. feature_set_1:1
   * @param entityNames list of entityName
   * @param entityDataSetRow entityDataSetRow to build the key from
   * @return {@link RedisKey}
   */
  private RedisKey makeRedisKey(String featureSet, List<String> entityNames,
      EntityDataSetRow entityDataSetRow) {
    try (Scope scope = tracer.buildSpan("Redis-makeRedisKey").startActive(true)) {
      RedisKey.Builder builder = RedisKey.newBuilder().setFeatureSet(featureSet);
      for (int i = 0; i < entityNames.size(); i++) {
        String entityName = entityNames.get(i);
        Value entityVal = entityDataSetRow.getValue(i);
        builder.addEntities(Field.newBuilder().setName(entityName).setValue(entityVal));
      }
      return builder.build();
    }
  }

}
