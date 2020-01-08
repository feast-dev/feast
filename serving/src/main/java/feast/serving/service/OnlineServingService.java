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
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract class OnlineServingService<LookupKeyType, ResponseType> implements ServingService {

  private final CachedSpecService specService;
  private final Tracer tracer;

  OnlineServingService(CachedSpecService specService, Tracer tracer) {
    this.specService = specService;
    this.tracer = tracer;
  }

  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest request) {
    try (Scope scope =
        tracer.buildSpan("OnlineServingService-getOnlineFeatures").startActive(true)) {
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

        sendAndProcessMultiGet(
            createLookupKeys(featureSetEntityNames, entityRows, featureSetRequest),
            entityRows,
            featureValuesMap,
            featureSetRequest);
      }
      List<FieldValues> fieldValues =
          featureValuesMap.values().stream()
              .map(m -> FieldValues.newBuilder().putAllFields(m).build())
              .collect(Collectors.toList());
      requestLatency.labels("getOnlineFeatures").observe(System.currentTimeMillis() - startTime);
      return getOnlineFeaturesResponseBuilder.addAllFieldValues(fieldValues).build();
    }
  }

  /**
   * Create lookup keys for corresponding data stores
   *
   * @param featureSetEntityNames list of entity names
   * @param entityRows list of {@link EntityRow}
   * @param featureSetRequest {@link FeatureSetRequest}
   * @return list of {@link LookupKeyType}
   */
  abstract List<LookupKeyType> createLookupKeys(
      List<String> featureSetEntityNames,
      List<EntityRow> entityRows,
      FeatureSetRequest featureSetRequest);

  /**
   * Checks whether the response is empty, i.e. feature does not exist in the store
   *
   * @param response {@link ResponseType}
   * @return boolean
   */
  protected abstract boolean isEmpty(ResponseType response);

  /**
   * Send a list of get requests
   *
   * @param keys list of {@link LookupKeyType}
   * @return list of {@link ResponseType}
   */
  protected abstract List<ResponseType> getAll(List<LookupKeyType> keys);

  /**
   * Parse response from data store to FeatureRow
   *
   * @param response {@link ResponseType}
   * @return {@link FeatureRow}
   */
  abstract FeatureRow parseResponse(ResponseType response) throws InvalidProtocolBufferException;

  private List<ResponseType> getResponses(List<LookupKeyType> keys) {
    try (Scope scope = tracer.buildSpan("OnlineServingService-sendMultiGet").startActive(true)) {
      long startTime = System.currentTimeMillis();
      try {
        return getAll(keys);
      } catch (Exception e) {
        throw Status.NOT_FOUND
            .withDescription("Unable to retrieve feature from online store")
            .withCause(e)
            .asRuntimeException();
      } finally {
        requestLatency.labels("sendMultiGet").observe(System.currentTimeMillis() - startTime);
      }
    }
  }

  private void sendAndProcessMultiGet(
      List<LookupKeyType> keys,
      List<EntityRow> entityRows,
      Map<EntityRow, Map<String, Value>> featureValuesMap,
      FeatureSetRequest featureSetRequest) {
    List<ResponseType> responses = getResponses(keys);

    long startTime = System.currentTimeMillis();
    try (Scope scope = tracer.buildSpan("OnlineServingService-processResponse").startActive(true)) {
      String featureSetId =
          String.format("%s:%d", featureSetRequest.getName(), featureSetRequest.getVersion());
      Map<String, Value> nullValues =
          featureSetRequest.getFeatureNamesList().stream()
              .collect(
                  Collectors.toMap(
                      name -> featureSetId + ":" + name, name -> Value.newBuilder().build()));
      for (int i = 0; i < responses.size(); i++) {
        EntityRow entityRow = entityRows.get(i);
        Map<String, Value> featureValues = featureValuesMap.get(entityRow);
        try {
          ResponseType response = responses.get(i);
          if (isEmpty(response)) {
            missingKeyCount.labels(featureSetRequest.getName()).inc();
            featureValues.putAll(nullValues);
            continue;
          }

          FeatureRow featureRow = parseResponse(response);
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
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
      }
    } finally {
      requestLatency.labels("processResponse").observe(System.currentTimeMillis() - startTime);
    }
  }

  private static boolean isStale(
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
}
