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
import feast.proto.serving.ServingAPIProto.FeastServingType;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesRequest;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.ValueType;
import feast.proto.types.ValueProto;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.specs.FeatureSpecRetriever;
import feast.serving.util.Metrics;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import io.grpc.Status;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class OnlineServingServiceV2 implements ServingServiceV2 {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingServiceV2.class);
  private final Tracer tracer;
  private final OnlineRetrieverV2 retriever;
  private final FeatureSpecRetriever featureSpecRetriever;
  private final OnlineTransformationService onlineTransformationService;

  public OnlineServingServiceV2(
      OnlineRetrieverV2 retriever,
      Tracer tracer,
      FeatureSpecRetriever featureSpecRetriever,
      OnlineTransformationService onlineTransformationService) {
    this.retriever = retriever;
    this.tracer = tracer;
    this.featureSpecRetriever = featureSpecRetriever;
    this.onlineTransformationService = onlineTransformationService;
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
    // Autofill default project if project is not specified
    String projectName = request.getProject();
    if (projectName.isEmpty()) {
      projectName = "default";
    }

    // Split all feature references into non-ODFV (e.g. batch and stream) references and ODFV.
    List<FeatureReferenceV2> allFeatureReferences = request.getFeaturesList();
    List<FeatureReferenceV2> featureReferences =
        allFeatureReferences.stream()
            .filter(r -> !this.featureSpecRetriever.isOnDemandFeatureReference(r))
            .collect(Collectors.toList());
    List<FeatureReferenceV2> onDemandFeatureReferences =
        allFeatureReferences.stream()
            .filter(r -> this.featureSpecRetriever.isOnDemandFeatureReference(r))
            .collect(Collectors.toList());

    // Get the set of request data feature names and feature inputs from the ODFV references.
    Pair<Set<String>, List<FeatureReferenceV2>> pair =
        this.onlineTransformationService.extractRequestDataFeatureNamesAndOnDemandFeatureInputs(
            onDemandFeatureReferences, projectName);
    Set<String> requestDataFeatureNames = pair.getLeft();
    List<FeatureReferenceV2> onDemandFeatureInputs = pair.getRight();

    // Add on demand feature inputs to list of feature references to retrieve.
    Set<FeatureReferenceV2> addedFeatureReferences = new HashSet<FeatureReferenceV2>();
    for (FeatureReferenceV2 onDemandFeatureInput : onDemandFeatureInputs) {
      if (!featureReferences.contains(onDemandFeatureInput)) {
        featureReferences.add(onDemandFeatureInput);
        addedFeatureReferences.add(onDemandFeatureInput);
      }
    }

    // Separate entity rows into entity data and request feature data.
    Pair<List<GetOnlineFeaturesRequestV2.EntityRow>, Map<String, List<ValueProto.Value>>>
        entityRowsAndRequestDataFeatures =
            this.onlineTransformationService.separateEntityRows(requestDataFeatureNames, request);
    List<GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        entityRowsAndRequestDataFeatures.getLeft();
    Map<String, List<ValueProto.Value>> requestDataFeatures =
        entityRowsAndRequestDataFeatures.getRight();
    // TODO: error checking on lengths of lists in entityRows and requestDataFeatures

    // Extract values and statuses to be used later in constructing FieldValues for the response.
    // The online features retrieved will augment these two data structures.
    List<Map<String, ValueProto.Value>> values =
        entityRows.stream().map(r -> new HashMap<>(r.getFieldsMap())).collect(Collectors.toList());
    List<Map<String, GetOnlineFeaturesResponse.FieldStatus>> statuses =
        entityRows.stream()
            .map(
                r ->
                    r.getFieldsMap().entrySet().stream()
                        .map(entry -> Pair.of(entry.getKey(), getMetadata(entry.getValue(), false)))
                        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight)))
            .collect(Collectors.toList());

    String finalProjectName = projectName;
    Map<FeatureReferenceV2, Duration> featureMaxAges =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> this.featureSpecRetriever.getMaxAge(finalProjectName, ref)));
    List<String> entityNames =
        featureReferences.stream()
            .map(ref -> this.featureSpecRetriever.getEntitiesList(finalProjectName, ref))
            .findFirst()
            .get();

    Map<FeatureReferenceV2, ValueProto.ValueType.Enum> featureValueTypes =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> {
                      try {
                        return this.featureSpecRetriever
                            .getFeatureSpec(finalProjectName, ref)
                            .getValueType();
                      } catch (SpecRetrievalException e) {
                        return ValueProto.ValueType.Enum.INVALID;
                      }
                    }));

    Span storageRetrievalSpan = tracer.buildSpan("storageRetrieval").start();
    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.setTag("entities", entityRows.size());
      storageRetrievalSpan.setTag("features", featureReferences.size());
    }
    List<List<Feature>> entityRowsFeatures =
        retriever.getOnlineFeatures(projectName, entityRows, featureReferences, entityNames);
    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.finish();
    }

    if (entityRowsFeatures.size() != entityRows.size()) {
      throw Status.INTERNAL
          .withDescription(
              "The no. of FeatureRow obtained from OnlineRetriever"
                  + "does not match no. of entityRow passed.")
          .asRuntimeException();
    }

    Span postProcessingSpan = tracer.buildSpan("postProcessing").start();

    for (int i = 0; i < entityRows.size(); i++) {
      GetOnlineFeaturesRequestV2.EntityRow entityRow = entityRows.get(i);
      List<Feature> curEntityRowFeatures = entityRowsFeatures.get(i);

      Map<FeatureReferenceV2, Feature> featureReferenceFeatureMap =
          getFeatureRefFeatureMap(curEntityRowFeatures);

      Map<String, ValueProto.Value> rowValues = values.get(i);
      Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);

      for (FeatureReferenceV2 featureReference : featureReferences) {
        if (featureReferenceFeatureMap.containsKey(featureReference)) {
          Feature feature = featureReferenceFeatureMap.get(featureReference);

          ValueProto.Value value =
              feature.getFeatureValue(featureValueTypes.get(feature.getFeatureReference()));

          Boolean isOutsideMaxAge =
              checkOutsideMaxAge(
                  feature, entityRow, featureMaxAges.get(feature.getFeatureReference()));

          if (!isOutsideMaxAge && value != null) {
            rowValues.put(FeatureV2.getFeatureStringRef(feature.getFeatureReference()), value);
          } else {
            rowValues.put(
                FeatureV2.getFeatureStringRef(feature.getFeatureReference()),
                ValueProto.Value.newBuilder().build());
          }

          rowStatuses.put(
              FeatureV2.getFeatureStringRef(feature.getFeatureReference()),
              getMetadata(value, isOutsideMaxAge));
        } else {
          rowValues.put(
              FeatureV2.getFeatureStringRef(featureReference),
              ValueProto.Value.newBuilder().build());

          rowStatuses.put(
              FeatureV2.getFeatureStringRef(featureReference), getMetadata(null, false));
        }
      }
      // Populate metrics/log request
      populateCountMetrics(rowStatuses, projectName);
    }

    if (postProcessingSpan != null) {
      postProcessingSpan.finish();
    }

    populateHistogramMetrics(entityRows, featureReferences, projectName);
    populateFeatureCountMetrics(featureReferences, projectName);

    // Handle ODFVs. For each ODFV reference, we send a TransformFeaturesRequest to the FTS.
    // The request should contain the entity data, the retrieved features, and the request data.
    if (!onDemandFeatureReferences.isEmpty()) {
      // Augment values, which contains the entity data and retrieved features, with the request
      // data. Also augment statuses.
      for (int i = 0; i < values.size(); i++) {
        Map<String, ValueProto.Value> rowValues = values.get(i);
        Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);

        for (Map.Entry<String, List<ValueProto.Value>> entry : requestDataFeatures.entrySet()) {
          String key = entry.getKey();
          List<ValueProto.Value> fieldValues = entry.getValue();
          rowValues.put(key, fieldValues.get(i));
          rowStatuses.put(key, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
        }
      }

      // Serialize the augmented values.
      ValueType transformationInput =
          this.onlineTransformationService.serializeValuesIntoArrowIPC(values);

      // Send out requests to the FTS and process the responses.
      Set<String> onDemandFeatureStringReferences =
          onDemandFeatureReferences.stream()
              .map(r -> FeatureV2.getFeatureStringRef(r))
              .collect(Collectors.toSet());
      for (FeatureReferenceV2 featureReference : onDemandFeatureReferences) {
        String onDemandFeatureViewName = featureReference.getFeatureTable();
        TransformFeaturesRequest transformFeaturesRequest =
            TransformFeaturesRequest.newBuilder()
                .setOnDemandFeatureViewName(onDemandFeatureViewName)
                .setProject(projectName)
                .setTransformationInput(transformationInput)
                .build();

        TransformFeaturesResponse transformFeaturesResponse =
            this.onlineTransformationService.transformFeatures(transformFeaturesRequest);

        this.onlineTransformationService.processTransformFeaturesResponse(
            transformFeaturesResponse,
            onDemandFeatureViewName,
            onDemandFeatureStringReferences,
            values,
            statuses);
      }

      // Remove all features that were added as inputs for ODFVs.
      Set<String> addedFeatureStringReferences =
          addedFeatureReferences.stream()
              .map(r -> FeatureV2.getFeatureStringRef(r))
              .collect(Collectors.toSet());
      for (int i = 0; i < values.size(); i++) {
        Map<String, ValueProto.Value> rowValues = values.get(i);
        Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);
        List<String> keysToRemove =
            rowValues.keySet().stream()
                .filter(k -> addedFeatureStringReferences.contains(k))
                .collect(Collectors.toList());
        for (String key : keysToRemove) {
          rowValues.remove(key);
          rowStatuses.remove(key);
        }
      }
    }

    // Build response field values from entityValuesMap and entityStatusesMap
    // Response field values should be in the same order as the entityRows provided by the user.
    List<GetOnlineFeaturesResponse.FieldValues> fieldValuesList =
        IntStream.range(0, entityRows.size())
            .mapToObj(
                entityRowIdx ->
                    GetOnlineFeaturesResponse.FieldValues.newBuilder()
                        .putAllFields(values.get(entityRowIdx))
                        .putAllStatuses(statuses.get(entityRowIdx))
                        .build())
            .collect(Collectors.toList());

    return GetOnlineFeaturesResponse.newBuilder().addAllFieldValues(fieldValuesList).build();
  }

  private static Map<FeatureReferenceV2, Feature> getFeatureRefFeatureMap(List<Feature> features) {
    return features.stream()
        .collect(Collectors.toMap(Feature::getFeatureReference, Function.identity()));
  }

  /**
   * Generate Field level Status metadata for the given valueMap.
   *
   * @param value value to generate metadata for.
   * @param isOutsideMaxAge whether the given valueMap contains values with age outside
   *     FeatureTable's max age.
   * @return a 1:1 map keyed by field name containing field status metadata instead of values in the
   *     given valueMap.
   */
  private static GetOnlineFeaturesResponse.FieldStatus getMetadata(
      ValueProto.Value value, boolean isOutsideMaxAge) {

    if (value == null) {
      return GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND;
    } else if (isOutsideMaxAge) {
      return GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE;
    } else if (value.getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
      return GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE;
    }
    return GetOnlineFeaturesResponse.FieldStatus.PRESENT;
  }

  /**
   * Determine if the feature data in the given feature row is outside maxAge. Data is outside
   * maxAge to be when the difference ingestion time set in feature row and the retrieval time set
   * in entity row exceeds FeatureTable max age.
   *
   * @param feature contains the ingestion timing and feature data.
   * @param entityRow contains the retrieval timing of when features are pulled.
   * @param maxAge feature's max age.
   */
  private static boolean checkOutsideMaxAge(
      Feature feature, GetOnlineFeaturesRequestV2.EntityRow entityRow, Duration maxAge) {

    if (maxAge.equals(Duration.getDefaultInstance())) { // max age is not set
      return false;
    }

    long givenTimestamp = entityRow.getTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - feature.getEventTimestamp().getSeconds();
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
    statusMap.forEach(
        (featureRefString, status) -> {
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
    featureReferences.forEach(
        featureReference ->
            Metrics.requestFeatureCount
                .labels(project, FeatureV2.getFeatureStringRef(featureReference))
                .inc());
  }
}
