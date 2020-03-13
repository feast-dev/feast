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

import static feast.serving.util.Metrics.invalidEncodingCount;
import static feast.serving.util.Metrics.missingKeyCount;
import static feast.serving.util.Metrics.requestCount;
import static feast.serving.util.Metrics.requestLatency;
import static feast.serving.util.Metrics.staleKeyCount;
import static feast.serving.util.RefUtil.generateFeatureSetStringRef;
import static feast.serving.util.RefUtil.generateFeatureStringRef;

import com.google.common.collect.Maps;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.FeatureReference;
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
import feast.serving.encoding.FeatureRowDecoder;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.FeatureSetRequest;
import feast.serving.util.RefUtil;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.grpc.Status;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class RedisServingService implements ServingService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisServingService.class);
  private final CachedSpecService specService;
  private final Tracer tracer;
  private final RedisCommands<byte[], byte[]> syncCommands;

  public RedisServingService(
      StatefulRedisConnection<byte[], byte[]> connection,
      CachedSpecService specService,
      Tracer tracer) {
    this.syncCommands = connection.sync();
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
      GetOnlineFeaturesResponse.Builder getOnlineFeaturesResponseBuilder =
          GetOnlineFeaturesResponse.newBuilder();

      List<EntityRow> entityRows = request.getEntityRowsList();
      Map<EntityRow, Map<String, Value>> featureValuesMap =
          entityRows.stream()
              .collect(Collectors.toMap(row -> row, row -> Maps.newHashMap(row.getFieldsMap())));
      List<FeatureSetRequest> featureSetRequests =
          specService.getFeatureSets(request.getFeaturesList());
      for (FeatureSetRequest featureSetRequest : featureSetRequests) {

        List<String> featureSetEntityNames =
            featureSetRequest.getSpec().getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());

        List<RedisKey> redisKeys =
            getRedisKeys(featureSetEntityNames, entityRows, featureSetRequest.getSpec());

        try {
          sendAndProcessMultiGet(redisKeys, entityRows, featureValuesMap, featureSetRequest);
        } catch (InvalidProtocolBufferException | ExecutionException e) {
          throw Status.INTERNAL
              .withDescription("Unable to parse protobuf while retrieving feature")
              .withCause(e)
              .asRuntimeException();
        }
      }
      List<FieldValues> fieldValues =
          featureValuesMap.values().stream()
              .map(valueMap -> FieldValues.newBuilder().putAllFields(valueMap).build())
              .collect(Collectors.toList());
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
   * @param featureSetSpec featureSetSpec of the features to retrieve
   * @return list of RedisKeys
   */
  private List<RedisKey> getRedisKeys(
      List<String> featureSetEntityNames,
      List<EntityRow> entityRows,
      FeatureSetSpec featureSetSpec) {
    try (Scope scope = tracer.buildSpan("Redis-makeRedisKeys").startActive(true)) {
      String featureSetRef = generateFeatureSetStringRef(featureSetSpec);
      List<RedisKey> redisKeys =
          entityRows.stream()
              .map(row -> makeRedisKey(featureSetRef, featureSetEntityNames, row))
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

  private void sendAndProcessMultiGet(
      List<RedisKey> redisKeys,
      List<EntityRow> entityRows,
      Map<EntityRow, Map<String, Value>> featureValuesMap,
      FeatureSetRequest featureSetRequest)
      throws InvalidProtocolBufferException, ExecutionException {

    List<byte[]> values = sendMultiGet(redisKeys);
    long startTime = System.currentTimeMillis();
    try (Scope scope = tracer.buildSpan("Redis-processResponse").startActive(true)) {
      FeatureSetSpec spec = featureSetRequest.getSpec();

      Map<String, Value> nullValues =
          featureSetRequest.getFeatureReferences().stream()
              .collect(
                  Collectors.toMap(
                      RefUtil::generateFeatureStringRef,
                      featureReference -> Value.newBuilder().build()));

      for (int i = 0; i < values.size(); i++) {
        EntityRow entityRow = entityRows.get(i);
        Map<String, Value> featureValues = featureValuesMap.get(entityRow);

        byte[] value = values.get(i);
        if (value == null) {
          featureSetRequest
              .getFeatureReferences()
              .parallelStream()
              .forEach(
                  request ->
                      missingKeyCount
                          .labels(
                              spec.getProject(),
                              String.format("%s:%d", request.getName(), request.getVersion()))
                          .inc());
          featureValues.putAll(nullValues);
          continue;
        }

        FeatureRow featureRow = FeatureRow.parseFrom(value);
        String featureSetRef = redisKeys.get(i).getFeatureSet();
        FeatureRowDecoder decoder =
            new FeatureRowDecoder(featureSetRef, specService.getFeatureSetSpec(featureSetRef));
        if (decoder.isEncoded(featureRow)) {
          if (decoder.isEncodingValid(featureRow)) {
            featureRow = decoder.decode(featureRow);
          } else {
            featureSetRequest
                .getFeatureReferences()
                .parallelStream()
                .forEach(
                    request ->
                        invalidEncodingCount
                            .labels(
                                spec.getProject(),
                                String.format("%s:%d", request.getName(), request.getVersion()))
                            .inc());
            featureValues.putAll(nullValues);
            continue;
          }
        }

        boolean stale = isStale(featureSetRequest, entityRow, featureRow);
        if (stale) {
          featureSetRequest
              .getFeatureReferences()
              .parallelStream()
              .forEach(
                  request ->
                      staleKeyCount
                          .labels(
                              spec.getProject(),
                              String.format("%s:%d", request.getName(), request.getVersion()))
                          .inc());
          featureValues.putAll(nullValues);
          continue;
        }

        featureSetRequest
            .getFeatureReferences()
            .parallelStream()
            .forEach(
                request ->
                    requestCount
                        .labels(
                            spec.getProject(),
                            String.format("%s:%d", request.getName(), request.getVersion()))
                        .inc());

        Map<String, FeatureReference> featureNames =
            featureSetRequest.getFeatureReferences().stream()
                .collect(
                    Collectors.toMap(
                        FeatureReference::getName, featureReference -> featureReference));
        featureRow.getFieldsList().stream()
            .filter(field -> featureNames.keySet().contains(field.getName()))
            .forEach(
                field -> {
                  FeatureReference ref = featureNames.get(field.getName());
                  String id = generateFeatureStringRef(ref);
                  featureValues.put(id, field.getValue());
                });
      }
    } finally {
      requestLatency
          .labels("processResponse")
          .observe((System.currentTimeMillis() - startTime) / 1000);
    }
  }

  private boolean isStale(
      FeatureSetRequest featureSetRequest, EntityRow entityRow, FeatureRow featureRow) {
    if (featureSetRequest.getSpec().getMaxAge().equals(Duration.getDefaultInstance())) {
      return false;
    }
    long givenTimestamp = entityRow.getEntityTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - featureRow.getEventTimestamp().getSeconds();
    return timeDifference > featureSetRequest.getSpec().getMaxAge().getSeconds();
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
      try {
        byte[][] binaryKeys =
            keys.stream()
                .map(AbstractMessageLite::toByteArray)
                .collect(Collectors.toList())
                .toArray(new byte[0][0]);
        return syncCommands.mget(binaryKeys).stream()
            .map(io.lettuce.core.Value::getValue)
            .collect(Collectors.toList());
      } catch (Exception e) {
        throw Status.NOT_FOUND
            .withDescription("Unable to retrieve feature from Redis")
            .withCause(e)
            .asRuntimeException();
      } finally {
        requestLatency
            .labels("sendMultiGet")
            .observe((System.currentTimeMillis() - startTime) / 1000d);
      }
    }
  }
}
