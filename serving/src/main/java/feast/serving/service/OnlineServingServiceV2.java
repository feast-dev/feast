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
package feast.serving.service;

import static feast.common.models.FeatureTable.getFeatureTableStringRef;

import com.google.protobuf.Duration;
import feast.common.models.FeatureV2;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.serving.ServingAPIProto.FeastServingType;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.types.ValueProto;
import feast.serving.specs.CachedSpecService;
import feast.serving.util.Metrics;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import io.grpc.Status;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class OnlineServingServiceV2 implements ServingServiceV2 {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingServiceV2.class);
  private final CachedSpecService specService;
  private final Tracer tracer;
  private final OnlineRetrieverV2 retriever;

  public OnlineServingServiceV2(
      OnlineRetrieverV2 retriever, CachedSpecService specService, Tracer tracer) {
    this.retriever = retriever;
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

  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequestV2 request) {
    String projectName = request.getProject();
    List<FeatureReferenceV2> featureReferences = request.getFeaturesList();

    // Autofill default project if project is not specified
    if (projectName.isEmpty()) {
      projectName = "default";
    }

    List<GetOnlineFeaturesRequestV2.EntityRow> entityRows = request.getEntityRowsList();
    // Collect the feature/entity value for each entity row in entityValueMap
    Map<GetOnlineFeaturesRequestV2.EntityRow, Map<String, ValueProto.Value>> entityValuesMap =
        entityRows.stream().collect(Collectors.toMap(row -> row, row -> new HashMap<>()));
    // Collect the feature/entity status metadata for each entity row in entityValueMap
    Map<GetOnlineFeaturesRequestV2.EntityRow, Map<String, GetOnlineFeaturesResponse.FieldStatus>>
        entityStatusesMap =
            entityRows.stream().collect(Collectors.toMap(row -> row, row -> new HashMap<>()));

    entityRows.forEach(
        entityRow -> {
          Map<String, ValueProto.Value> valueMap = entityRow.getFieldsMap();
          entityValuesMap.get(entityRow).putAll(valueMap);
          entityStatusesMap.get(entityRow).putAll(getMetadataMap(valueMap, false, false));
        });

    Span onlineRetrievalSpan = tracer.buildSpan("onlineRetrieval").start();
    if (onlineRetrievalSpan != null) {
      onlineRetrievalSpan.setTag("entities", entityRows.size());
      onlineRetrievalSpan.setTag("features", featureReferences.size());
    }
    List<List<Optional<Feature>>> entityRowsFeatures =
        retriever.getOnlineFeatures(projectName, entityRows, featureReferences);
    if (onlineRetrievalSpan != null) {
      onlineRetrievalSpan.finish();
    }

    if (entityRowsFeatures.size() != entityRows.size()) {
      throw Status.INTERNAL
          .withDescription(
              "The no. of FeatureRow obtained from OnlineRetriever"
                  + "does not match no. of entityRow passed.")
          .asRuntimeException();
    }

    for (int i = 0; i < entityRows.size(); i++) {
      GetOnlineFeaturesRequestV2.EntityRow entityRow = entityRows.get(i);
      List<Optional<Feature>> curEntityRowFeatures = entityRowsFeatures.get(i);

      Map<FeatureReferenceV2, Optional<Feature>> featureReferenceFeatureMap =
          getFeatureRefFeatureMap(curEntityRowFeatures);

      Map<String, ValueProto.Value> allValueMaps = new HashMap<>();
      Map<String, GetOnlineFeaturesResponse.FieldStatus> allStatusMaps = new HashMap<>();

      for (FeatureReferenceV2 featureReference : featureReferences) {
        if (featureReferenceFeatureMap.containsKey(featureReference)) {
          Optional<Feature> feature = featureReferenceFeatureMap.get(featureReference);

          FeatureTableSpec featureTableSpec =
              specService.getFeatureTableSpec(projectName, feature.get().getFeatureReference());
          FeatureProto.FeatureSpecV2 featureSpec =
              specService.getFeatureSpec(projectName, feature.get().getFeatureReference());
          ValueProto.ValueType.Enum valueTypeEnum = featureSpec.getValueType();
          ValueProto.Value.ValCase valueCase = feature.get().getFeatureValue().getValCase();
          boolean isMatchingFeatureSpec = checkSameFeatureSpec(valueTypeEnum, valueCase);

          boolean isOutsideMaxAge = checkOutsideMaxAge(featureTableSpec, entityRow, feature);
          Map<String, ValueProto.Value> valueMap =
              unpackValueMap(feature, isOutsideMaxAge, isMatchingFeatureSpec);
          allValueMaps.putAll(valueMap);

          // Generate metadata for feature values and merge into entityFieldsMap
          Map<String, GetOnlineFeaturesResponse.FieldStatus> statusMap =
              getMetadataMap(valueMap, !isMatchingFeatureSpec, isOutsideMaxAge);
          allStatusMaps.putAll(statusMap);

          // Populate metrics/log request
          populateCountMetrics(statusMap, projectName);
        } else {
          Map<String, ValueProto.Value> valueMap =
              new HashMap<>() {
                {
                  put(
                      FeatureV2.getFeatureStringRef(featureReference),
                      ValueProto.Value.newBuilder().build());
                }
              };
          allValueMaps.putAll(valueMap);

          Map<String, GetOnlineFeaturesResponse.FieldStatus> statusMap =
              getMetadataMap(valueMap, true, false);
          allStatusMaps.putAll(statusMap);

          // Populate metrics/log request
          populateCountMetrics(statusMap, projectName);
        }
      }
      entityValuesMap.get(entityRow).putAll(allValueMaps);
      entityStatusesMap.get(entityRow).putAll(allStatusMaps);
    }
    populateHistogramMetrics(entityRows, featureReferences, projectName);
    populateFeatureCountMetrics(featureReferences, projectName);

    // Build response field values from entityValuesMap and entityStatusesMap
    // Response field values should be in the same order as the entityRows provided by the user.
    List<GetOnlineFeaturesResponse.FieldValues> fieldValuesList =
        entityRows.stream()
            .map(
                entityRow -> {
                  return GetOnlineFeaturesResponse.FieldValues.newBuilder()
                      .putAllFields(entityValuesMap.get(entityRow))
                      .putAllStatuses(entityStatusesMap.get(entityRow))
                      .build();
                })
            .collect(Collectors.toList());
    return GetOnlineFeaturesResponse.newBuilder().addAllFieldValues(fieldValuesList).build();
  }

  private boolean checkSameFeatureSpec(
      ValueProto.ValueType.Enum valueTypeEnum, ValueProto.Value.ValCase valueCase) {
    HashMap<ValueProto.ValueType.Enum, ValueProto.Value.ValCase> typingMap =
        new HashMap<>() {
          {
            put(ValueProto.ValueType.Enum.BYTES, ValueProto.Value.ValCase.BYTES_VAL);
            put(ValueProto.ValueType.Enum.STRING, ValueProto.Value.ValCase.STRING_VAL);
            put(ValueProto.ValueType.Enum.INT32, ValueProto.Value.ValCase.INT32_VAL);
            put(ValueProto.ValueType.Enum.INT64, ValueProto.Value.ValCase.INT64_VAL);
            put(ValueProto.ValueType.Enum.DOUBLE, ValueProto.Value.ValCase.DOUBLE_VAL);
            put(ValueProto.ValueType.Enum.FLOAT, ValueProto.Value.ValCase.FLOAT_VAL);
            put(ValueProto.ValueType.Enum.BOOL, ValueProto.Value.ValCase.BOOL_VAL);
            put(ValueProto.ValueType.Enum.BYTES_LIST, ValueProto.Value.ValCase.BYTES_LIST_VAL);
            put(ValueProto.ValueType.Enum.STRING_LIST, ValueProto.Value.ValCase.STRING_LIST_VAL);
            put(ValueProto.ValueType.Enum.INT32_LIST, ValueProto.Value.ValCase.INT32_LIST_VAL);
            put(ValueProto.ValueType.Enum.INT64_LIST, ValueProto.Value.ValCase.INT64_LIST_VAL);
            put(ValueProto.ValueType.Enum.DOUBLE_LIST, ValueProto.Value.ValCase.DOUBLE_LIST_VAL);
            put(ValueProto.ValueType.Enum.FLOAT_LIST, ValueProto.Value.ValCase.FLOAT_LIST_VAL);
            put(ValueProto.ValueType.Enum.BOOL_LIST, ValueProto.Value.ValCase.BOOL_LIST_VAL);
          }
        };
    if (valueCase.equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
      return true;
    }

    return typingMap.get(valueTypeEnum).equals(valueCase);
  }

  private static Map<FeatureReferenceV2, Optional<Feature>> getFeatureRefFeatureMap(
      List<Optional<Feature>> features) {
    Map<FeatureReferenceV2, Optional<Feature>> featureReferenceFeatureMap = new HashMap<>();
    features.forEach(
        feature -> {
          FeatureReferenceV2 featureReference = feature.get().getFeatureReference();
          featureReferenceFeatureMap.put(featureReference, feature);
        });

    return featureReferenceFeatureMap;
  }

  /**
   * Generate Field level Status metadata for the given valueMap.
   *
   * @param valueMap map of field name to value to generate metadata for.
   * @param isNotFound whether the given valueMap represents values that were not found in the
   *     online retriever.
   * @param isOutsideMaxAge whether the given valueMap contains values with age outside
   *     FeatureTable's max age.
   * @return a 1:1 map keyed by field name containing field status metadata instead of values in the
   *     given valueMap.
   */
  private static Map<String, GetOnlineFeaturesResponse.FieldStatus> getMetadataMap(
      Map<String, ValueProto.Value> valueMap, boolean isNotFound, boolean isOutsideMaxAge) {
    return valueMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                es -> es.getKey(),
                es -> {
                  ValueProto.Value fieldValue = es.getValue();
                  if (isNotFound) {
                    return GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND;
                  } else if (isOutsideMaxAge) {
                    return GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE;
                  } else if (fieldValue.getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
                    return GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE;
                  }
                  return GetOnlineFeaturesResponse.FieldStatus.PRESENT;
                }));
  }

  private static Map<String, ValueProto.Value> unpackValueMap(
      Optional<Feature> feature, boolean isOutsideMaxAge, boolean isMatchingFeatureSpec) {
    Map<String, ValueProto.Value> valueMap = new HashMap<>();

    if (feature.isPresent()) {
      if (!isOutsideMaxAge && isMatchingFeatureSpec) {
        valueMap.put(
            FeatureV2.getFeatureStringRef(feature.get().getFeatureReference()),
            feature.get().getFeatureValue());
      } else {
        valueMap.put(
            FeatureV2.getFeatureStringRef(feature.get().getFeatureReference()),
            ValueProto.Value.newBuilder().build());
      }
    }
    return valueMap;
  }

  /**
   * Determine if the feature data in the given feature row is outside maxAge. Data is outside
   * maxAge to be when the difference ingestion time set in feature row and the retrieval time set
   * in entity row exceeds FeatureTable max age.
   *
   * @param featureTableSpec contains the spec where feature's max age is extracted.
   * @param entityRow contains the retrieval timing of when features are pulled.
   * @param feature contains the ingestion timing and feature data.
   */
  private static boolean checkOutsideMaxAge(
      FeatureTableSpec featureTableSpec,
      GetOnlineFeaturesRequestV2.EntityRow entityRow,
      Optional<Feature> feature) {
    Duration maxAge = featureTableSpec.getMaxAge();
    if (feature.isEmpty()) { // no data to consider
      return false;
    }
    if (maxAge.equals(Duration.getDefaultInstance())) { // max age is not set
      return false;
    }

    long givenTimestamp = entityRow.getTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - feature.get().getEventTimestamp().getSeconds();
    return timeDifference > maxAge.getSeconds();
  }

  /**
   * Populate histogram metrics that can be used for analysing online retrieval calls
   *
   * @param entityRows entity rows provided in request
   * @param featureReferences feature references provided in request
   * @param project project name provided in request
   */
  private void populateHistogramMetrics(
      List<GetOnlineFeaturesRequestV2.EntityRow> entityRows,
      List<FeatureReferenceV2> featureReferences,
      String project) {
    Metrics.requestEntityCountDistribution
        .labels(project)
        .observe(Double.valueOf(entityRows.size()));
    Metrics.requestFeatureCountDistribution
        .labels(project)
        .observe(Double.valueOf(featureReferences.size()));

    long countDistinctFeatureTables =
        featureReferences.stream()
            .map(featureReference -> getFeatureTableStringRef(project, featureReference))
            .distinct()
            .count();
    Metrics.requestFeatureTableCountDistribution
        .labels(project)
        .observe(Double.valueOf(countDistinctFeatureTables));
  }

  /**
   * Populate count metrics that can be used for analysing online retrieval calls
   *
   * @param statusMap Statuses of features which have been requested
   * @param project Project where request for features was called from
   */
  private void populateCountMetrics(
      Map<String, GetOnlineFeaturesResponse.FieldStatus> statusMap, String project) {
    statusMap
        .entrySet()
        .forEach(
            es -> {
              String featureRefString = es.getKey();
              GetOnlineFeaturesResponse.FieldStatus status = es.getValue();
              if (status == GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND) {
                Metrics.notFoundKeyCount.labels(project, featureRefString).inc();
              }
              if (status == GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE) {
                Metrics.staleKeyCount.labels(project, featureRefString).inc();
              }
            });
  }

  private void populateFeatureCountMetrics(
      List<FeatureReferenceV2> featureReferences, String project) {
    featureReferences
        .parallelStream()
        .forEach(
            featureReference ->
                Metrics.requestFeatureCount
                    .labels(project, FeatureV2.getFeatureStringRef(featureReference))
                    .inc());
  }
}
