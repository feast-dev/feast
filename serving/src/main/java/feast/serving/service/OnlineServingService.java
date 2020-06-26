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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import feast.common.models.Feature;
import feast.common.models.FeatureSet;
import feast.proto.serving.ServingAPIProto.*;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.serving.specs.CachedSpecService;
import feast.serving.util.Metrics;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class OnlineServingService implements ServingService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingService.class);
  private final CachedSpecService specService;
  private final Tracer tracer;
  private final OnlineRetriever retriever;

  public OnlineServingService(
      OnlineRetriever retriever, CachedSpecService specService, Tracer tracer) {
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

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("getOnlineFeatures").startActive(true)) {
      List<EntityRow> entityRows = request.getEntityRowsList();
      // Collect the feature/entity value for each entity row in entityValueMap
      Map<EntityRow, Map<String, Value>> entityValuesMap =
          entityRows.stream().collect(Collectors.toMap(row -> row, row -> new HashMap<>()));
      // Collect the feature/entity status metadata for each entity row in entityValueMap
      Map<EntityRow, Map<String, FieldStatus>> entityStatusesMap =
          entityRows.stream().collect(Collectors.toMap(row -> row, row -> new HashMap<>()));
      // Collect featureRows retrieved for logging/tracing
      List<List<Optional<FeatureRow>>> logFeatureRows = new LinkedList<>();

      if (!request.getOmitEntitiesInResponse()) {
        // Add entity row's fields as response fields
        entityRows.forEach(
            entityRow -> {
              Map<String, Value> valueMap = entityRow.getFieldsMap();
              entityValuesMap.get(entityRow).putAll(valueMap);
              entityStatusesMap.get(entityRow).putAll(getMetadataMap(valueMap, false, false));
            });
      }

      List<FeatureSetRequest> featureSetRequests =
          specService.getFeatureSets(request.getFeaturesList(), request.getProject());
      for (FeatureSetRequest featureSetRequest : featureSetRequests) {
        // Pull feature rows for given entity rows from the feature/featureset specified in feature
        // set request.
        // from the configured online
        List<Optional<FeatureRow>> featureRows =
            retriever.getOnlineFeatures(entityRows, featureSetRequest);
        // Check that feature row returned corresponds to a given entity row.
        if (featureRows.size() != entityRows.size()) {
          throw Status.INTERNAL
              .withDescription(
                  "The no. of FeatureRow obtained from OnlineRetriever"
                      + "does not match no. of entityRow passed.")
              .asRuntimeException();
        }

        Streams.zip(entityRows.stream(), featureRows.stream(), Pair::of)
            .forEach(
                entityFeaturePair -> {
                  EntityRow entityRow = entityFeaturePair.getLeft();
                  Optional<FeatureRow> featureRow = entityFeaturePair.getRight();
                  // Unpack feature field values and merge into entityValueMap
                  boolean isOutsideMaxAge =
                      checkOutsideMaxAge(featureSetRequest, entityRow, featureRow);
                  Map<String, Value> valueMap =
                      unpackValueMap(featureRow, featureSetRequest, isOutsideMaxAge);
                  entityValuesMap.get(entityRow).putAll(valueMap);

                  // Generate metadata for feature values and merge into entityFieldsMap
                  boolean isNotFound = featureRow.isEmpty();
                  Map<String, FieldStatus> statusMap =
                      getMetadataMap(valueMap, isNotFound, isOutsideMaxAge);
                  entityStatusesMap.get(entityRow).putAll(statusMap);

                  // Populate metrics/log request
                  populateCountMetrics(statusMap, featureSetRequest);
                });
        populateRequestCountMetrics(featureSetRequest);
        logFeatureRows.add(featureRows);
      }
      if (scope != null) {
        logFeatureRowsTrace(scope, logFeatureRows, featureSetRequests);
      }

      // Build response field values from entityValuesMap and entityStatusesMap
      // Reponse field values should be in the same order as the entityRows provided by the user.
      List<FieldValues> fieldValuesList =
          entityRows.stream()
              .map(
                  entityRow -> {
                    return FieldValues.newBuilder()
                        .putAllFields(entityValuesMap.get(entityRow))
                        .putAllStatuses(entityStatusesMap.get(entityRow))
                        .build();
                  })
              .collect(Collectors.toList());
      return GetOnlineFeaturesResponse.newBuilder().addAllFieldValues(fieldValuesList).build();
    }
  }

  /**
   * Unpack feature values using data from the given feature row for features specified in the given
   * feature set request.
   *
   * @param featureRow optional to unpack for feature values.
   * @param featureSetRequest feature set request for which the feature row is retrieved for.
   * @param isOutsideMaxAge whether which the feature row contains values that is outside max age.
   * @return valueMap mapping string feature name to feature value for the given feature set
   *     request.
   */
  private static Map<String, Value> unpackValueMap(
      Optional<FeatureRow> featureRow,
      FeatureSetRequest featureSetRequest,
      boolean isOutsideMaxAge) {
    Map<String, Value> valueMap = new HashMap<>();
    // In order to return values containing the same feature references provided by the user,
    // we reuse the feature references in the request as the keys in field builder map
    Map<String, FeatureReference> nameRefMap = featureSetRequest.getFeatureRefsByName();

    if (featureRow.isPresent()) {
      // unpack feature row's feature values and populate value map
      Map<String, Value> featureValueMap =
          featureRow.get().getFieldsList().stream()
              .filter(featureRowField -> nameRefMap.containsKey(featureRowField.getName()))
              .collect(
                  Collectors.toMap(
                      featureRowField -> {
                        FeatureReference featureRef = nameRefMap.get(featureRowField.getName());
                        return Feature.getFeatureStringWithProjectRef(featureRef);
                      },
                      featureRowField -> {
                        // drop feature values with an age outside feature set's max age.
                        return (isOutsideMaxAge)
                            ? Value.newBuilder().build()
                            : featureRowField.getValue();
                      }));
      valueMap.putAll(featureValueMap);
    }
    // create empty values for features specified in request but not present in feature row.
    Set<String> missingFeatures =
        nameRefMap.values().stream()
            .map(ref -> Feature.getFeatureStringWithProjectRef(ref))
            .collect(Collectors.toSet());
    missingFeatures.removeAll(valueMap.keySet());
    missingFeatures.forEach(refString -> valueMap.put(refString, Value.newBuilder().build()));

    return valueMap;
  }

  /**
   * Generate Field level Status metadata for the given valueMap.
   *
   * @param valueMap map of field name to value to generate metadata for.
   * @param isNotFound whether the given valueMap represents values that were not found in the
   *     online retriever.
   * @param isOutsideMaxAge whether the given valueMap contains values with age outside feature
   *     set's max age.
   * @return a 1:1 map keyed by field name containing field status metadata instead of values in the
   *     given valueMap.
   */
  private static Map<String, FieldStatus> getMetadataMap(
      Map<String, Value> valueMap, boolean isNotFound, boolean isOutsideMaxAge) {
    return valueMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                es -> es.getKey(),
                es -> {
                  Value fieldValue = es.getValue();
                  if (isNotFound) {
                    return FieldStatus.NOT_FOUND;
                  } else if (isOutsideMaxAge) {
                    return FieldStatus.OUTSIDE_MAX_AGE;
                  } else if (fieldValue.getValCase().equals(Value.ValCase.VAL_NOT_SET)) {
                    return FieldStatus.NULL_VALUE;
                  }
                  return FieldStatus.PRESENT;
                }));
  }

  /**
   * Determine if the feature data in the given feature row is outside maxAge. Data is outside
   * maxAge to be when the difference ingestion time set in feature row and the retrieval time set
   * in entity row exceeds featureset max age.
   *
   * @param featureSetRequest contains the spec where feature's max age is extracted.
   * @param entityRow contains the retrieval timing of when features are pulled.
   * @param featureRow contains the ingestion timing and feature data.
   */
  private static boolean checkOutsideMaxAge(
      FeatureSetRequest featureSetRequest, EntityRow entityRow, Optional<FeatureRow> featureRow) {
    Duration maxAge = featureSetRequest.getSpec().getMaxAge();
    if (featureRow.isEmpty()) { // no data to consider
      return false;
    }
    if (maxAge.equals(Duration.getDefaultInstance())) { // max age is not set
      return false;
    }

    long givenTimestamp = entityRow.getEntityTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - featureRow.get().getEventTimestamp().getSeconds();
    return timeDifference > maxAge.getSeconds();
  }

  private void logFeatureRowsTrace(
      Scope scope,
      List<List<Optional<FeatureRow>>> logFeatureRows,
      List<FeatureSetRequest> featureSetRequests) {
    List<List<FeatureRow>> loggableFeatureRows =
        Streams.zip(
                logFeatureRows.stream(),
                featureSetRequests.stream(),
                (featureRows, featureSetRequest) -> {
                  FeatureRow.Builder nullFeatureRowBuilder =
                      FeatureRow.newBuilder()
                          .setFeatureSet(
                              FeatureSet.getFeatureSetStringRef(featureSetRequest.getSpec()));
                  for (FeatureReference featureReference :
                      featureSetRequest.getFeatureReferences()) {
                    nullFeatureRowBuilder.addFields(
                        Field.newBuilder().setName(featureReference.getName()));
                  }

                  // log null feature row when feature row is empty
                  return featureRows.stream()
                      .map(
                          featureRow -> {
                            return (featureRow.isEmpty())
                                ? nullFeatureRowBuilder.build()
                                : featureRow.get();
                          })
                      .collect(Collectors.toList());
                })
            .collect(Collectors.toList());

    scope.span().log(ImmutableMap.of("event", "featureRows", "value", loggableFeatureRows));
  }

  private void populateCountMetrics(
      Map<String, FieldStatus> statusMap, FeatureSetRequest featureSetRequest) {
    String project = featureSetRequest.getSpec().getProject();
    statusMap
        .entrySet()
        .forEach(
            es -> {
              String featureRefString = es.getKey();
              FieldStatus status = es.getValue();
              if (status == FieldStatus.NOT_FOUND) {
                Metrics.notFoundKeyCount.labels(project, featureRefString).inc();
              }
              if (status == FieldStatus.OUTSIDE_MAX_AGE) {
                Metrics.staleKeyCount.labels(project, featureRefString).inc();
              }
            });
  }

  private void populateRequestCountMetrics(FeatureSetRequest featureSetRequest) {
    String project = featureSetRequest.getSpec().getProject();
    featureSetRequest
        .getFeatureReferences()
        .parallelStream()
        .forEach(ref -> Metrics.requestCount.labels(project, ref.getName()).inc());
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }
}
