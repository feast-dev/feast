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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.common.models.Feature;
import feast.proto.core.FeatureServiceProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesRequest;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.ValueType;
import feast.proto.types.ValueProto;
import feast.serving.registry.RegistryRepository;
import feast.serving.util.Metrics;
import feast.storage.api.retriever.OnlineRetrieverV2;
import io.grpc.Status;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class OnlineServingServiceV2 implements ServingServiceV2 {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingServiceV2.class);
  private final Tracer tracer;
  private final OnlineRetrieverV2 retriever;
  private final RegistryRepository registryRepository;
  private final OnlineTransformationService onlineTransformationService;
  private final String project;

  public OnlineServingServiceV2(
      OnlineRetrieverV2 retriever,
      Tracer tracer,
      RegistryRepository registryRepository,
      OnlineTransformationService onlineTransformationService,
      String project) {
    this.retriever = retriever;
    this.tracer = tracer;
    this.registryRepository = registryRepository;
    this.onlineTransformationService = onlineTransformationService;
    this.project = project;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.getDefaultInstance();
  }

  @Override
  public ServingAPIProto.GetOnlineFeaturesResponse getOnlineFeatures(
      ServingAPIProto.GetOnlineFeaturesRequest request) {
    // Split all feature references into non-ODFV (e.g. batch and stream) references and ODFV.
    List<FeatureReferenceV2> allFeatureReferences = getFeaturesList(request);
    List<FeatureReferenceV2> retrievedFeatureReferences =
        allFeatureReferences.stream()
            .filter(r -> !this.registryRepository.isOnDemandFeatureReference(r))
            .collect(Collectors.toList());
    int userRequestedFeaturesSize = retrievedFeatureReferences.size();

    List<FeatureReferenceV2> onDemandFeatureReferences =
        allFeatureReferences.stream()
            .filter(r -> this.registryRepository.isOnDemandFeatureReference(r))
            .collect(Collectors.toList());

    // ToDo (pyalex): refactor transformation service to delete unused left part of the returned
    // Pair from extractRequestDataFeatureNamesAndOnDemandFeatureInputs.
    // Currently, we can retrieve context variables directly from GetOnlineFeaturesRequest.
    List<FeatureReferenceV2> onDemandFeatureInputs =
        this.onlineTransformationService.extractOnDemandFeaturesDependencies(
            onDemandFeatureReferences);

    // Add on demand feature inputs to list of feature references to retrieve.
    for (FeatureReferenceV2 onDemandFeatureInput : onDemandFeatureInputs) {
      if (!retrievedFeatureReferences.contains(onDemandFeatureInput)) {
        retrievedFeatureReferences.add(onDemandFeatureInput);
      }
    }

    List<Map<String, ValueProto.Value>> entityRows = getEntityRows(request);

    List<String> entityNames;
    if (retrievedFeatureReferences.size() > 0) {
      entityNames = this.registryRepository.getEntitiesList(retrievedFeatureReferences.get(0));
    } else {
      throw new RuntimeException("Requested features list must not be empty");
    }

    Span storageRetrievalSpan = tracer.buildSpan("storageRetrieval").start();
    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.setTag("entities", entityRows.size());
      storageRetrievalSpan.setTag("features", retrievedFeatureReferences.size());
    }
    List<List<feast.storage.api.retriever.Feature>> features =
        retriever.getOnlineFeatures(entityRows, retrievedFeatureReferences, entityNames);

    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.finish();
    }
    if (features.size() != entityRows.size()) {
      throw Status.INTERNAL
          .withDescription(
              "The no. of FeatureRow obtained from OnlineRetriever"
                  + "does not match no. of entityRow passed.")
          .asRuntimeException();
    }

    Span postProcessingSpan = tracer.buildSpan("postProcessing").start();

    ServingAPIProto.GetOnlineFeaturesResponse.Builder responseBuilder =
        ServingAPIProto.GetOnlineFeaturesResponse.newBuilder();

    Timestamp now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();
    Timestamp nullTimestamp = Timestamp.newBuilder().build();
    ValueProto.Value nullValue = ValueProto.Value.newBuilder().build();

    for (int featureIdx = 0; featureIdx < userRequestedFeaturesSize; featureIdx++) {
      FeatureReferenceV2 featureReference = retrievedFeatureReferences.get(featureIdx);

      ValueProto.ValueType.Enum valueType =
          this.registryRepository.getFeatureSpec(featureReference).getValueType();

      Duration maxAge = this.registryRepository.getMaxAge(featureReference);

      ServingAPIProto.GetOnlineFeaturesResponse.FeatureVector.Builder vectorBuilder =
          responseBuilder.addResultsBuilder();

      for (int rowIdx = 0; rowIdx < features.size(); rowIdx++) {
        feast.storage.api.retriever.Feature feature = features.get(rowIdx).get(featureIdx);
        if (feature == null) {
          vectorBuilder.addValues(nullValue);
          vectorBuilder.addStatuses(FieldStatus.NOT_FOUND);
          vectorBuilder.addEventTimestamps(nullTimestamp);
          continue;
        }

        ValueProto.Value featureValue = feature.getFeatureValue(valueType);
        if (featureValue == null) {
          vectorBuilder.addValues(nullValue);
          vectorBuilder.addStatuses(FieldStatus.NOT_FOUND);
          vectorBuilder.addEventTimestamps(nullTimestamp);
          continue;
        }

        vectorBuilder.addValues(featureValue);
        vectorBuilder.addStatuses(
            getFeatureStatus(featureValue, checkOutsideMaxAge(feature, now, maxAge)));
        vectorBuilder.addEventTimestamps(feature.getEventTimestamp());
      }

      populateCountMetrics(featureReference, vectorBuilder);
    }

    responseBuilder.setMetadata(
        ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
            .setFeatureNames(
                ServingAPIProto.FeatureList.newBuilder()
                    .addAllVal(
                        retrievedFeatureReferences.stream()
                            .map(Feature::getFeatureReference)
                            .collect(Collectors.toList()))));

    if (postProcessingSpan != null) {
      postProcessingSpan.finish();
    }

    if (!onDemandFeatureReferences.isEmpty()) {
      // Handle ODFVs. For each ODFV reference, we send a TransformFeaturesRequest to the FTS.
      // The request should contain the entity data, the retrieved features, and the request context
      // data.
      this.populateOnDemandFeatures(
          onDemandFeatureReferences,
          onDemandFeatureInputs,
          retrievedFeatureReferences,
          request,
          features,
          responseBuilder);
    }

    populateHistogramMetrics(entityRows, retrievedFeatureReferences);
    populateFeatureCountMetrics(retrievedFeatureReferences);

    return responseBuilder.build();
  }

  private List<FeatureReferenceV2> getFeaturesList(
      ServingAPIProto.GetOnlineFeaturesRequest request) {
    if (request.getFeatures().getValCount() > 0) {
      return request.getFeatures().getValList().stream()
          .map(Feature::parseFeatureReference)
          .collect(Collectors.toList());
    }

    FeatureServiceProto.FeatureServiceSpec featureServiceSpec =
        this.registryRepository.getFeatureServiceSpec(request.getFeatureService());

    return featureServiceSpec.getFeaturesList().stream()
        .flatMap(
            featureViewProjection ->
                featureViewProjection.getFeatureColumnsList().stream()
                    .map(
                        f ->
                            FeatureReferenceV2.newBuilder()
                                .setFeatureViewName(featureViewProjection.getFeatureViewName())
                                .setFeatureName(f.getName())
                                .build()))
        .collect(Collectors.toList());
  }

  private List<Map<String, ValueProto.Value>> getEntityRows(
      ServingAPIProto.GetOnlineFeaturesRequest request) {
    if (request.getEntitiesCount() == 0) {
      throw new RuntimeException("Entities map shouldn't be empty");
    }

    Set<String> entityNames = request.getEntitiesMap().keySet();
    String firstEntity = entityNames.stream().findFirst().get();
    int rowsCount = request.getEntitiesMap().get(firstEntity).getValCount();
    List<Map<String, ValueProto.Value>> entityRows = Lists.newArrayListWithExpectedSize(rowsCount);

    for (Map.Entry<String, ValueProto.RepeatedValue> entity : request.getEntitiesMap().entrySet()) {
      for (int i = 0; i < rowsCount; i++) {
        if (entityRows.size() < i + 1) {
          entityRows.add(i, Maps.newHashMapWithExpectedSize(entityNames.size()));
        }

        entityRows.get(i).put(entity.getKey(), entity.getValue().getVal(i));
      }
    }

    return entityRows;
  }

  private void populateOnDemandFeatures(
      List<FeatureReferenceV2> onDemandFeatureReferences,
      List<FeatureReferenceV2> onDemandFeatureInputs,
      List<FeatureReferenceV2> retrievedFeatureReferences,
      ServingAPIProto.GetOnlineFeaturesRequest request,
      List<List<feast.storage.api.retriever.Feature>> features,
      ServingAPIProto.GetOnlineFeaturesResponse.Builder responseBuilder) {

    List<Pair<String, List<ValueProto.Value>>> onDemandContext =
        request.getRequestContextMap().entrySet().stream()
            .map(e -> Pair.of(e.getKey(), e.getValue().getValList()))
            .collect(Collectors.toList());

    for (int featureIdx = 0; featureIdx < retrievedFeatureReferences.size(); featureIdx++) {
      FeatureReferenceV2 featureReference = retrievedFeatureReferences.get(featureIdx);

      if (!onDemandFeatureInputs.contains(featureReference)) {
        continue;
      }

      ValueProto.ValueType.Enum valueType =
          this.registryRepository.getFeatureSpec(featureReference).getValueType();

      List<ValueProto.Value> valueList = Lists.newArrayListWithExpectedSize(features.size());
      for (int rowIdx = 0; rowIdx < features.size(); rowIdx++) {
        valueList.add(features.get(rowIdx).get(featureIdx).getFeatureValue(valueType));
      }

      onDemandContext.add(
          Pair.of(
              String.format(
                  "%s__%s",
                  featureReference.getFeatureViewName(), featureReference.getFeatureName()),
              valueList));
    }
    // Serialize the augmented values.
    ValueType transformationInput =
        this.onlineTransformationService.serializeValuesIntoArrowIPC(onDemandContext);

    // Send out requests to the FTS and process the responses.
    Set<String> onDemandFeatureStringReferences =
        onDemandFeatureReferences.stream()
            .map(r -> Feature.getFeatureReference(r))
            .collect(Collectors.toSet());

    for (FeatureReferenceV2 featureReference : onDemandFeatureReferences) {
      String onDemandFeatureViewName = featureReference.getFeatureViewName();
      TransformFeaturesRequest transformFeaturesRequest =
          TransformFeaturesRequest.newBuilder()
              .setOnDemandFeatureViewName(onDemandFeatureViewName)
              .setTransformationInput(transformationInput)
              .build();

      TransformFeaturesResponse transformFeaturesResponse =
          this.onlineTransformationService.transformFeatures(transformFeaturesRequest);

      this.onlineTransformationService.processTransformFeaturesResponse(
          transformFeaturesResponse,
          onDemandFeatureViewName,
          onDemandFeatureStringReferences,
          responseBuilder);
    }
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
  private static FieldStatus getFeatureStatus(ValueProto.Value value, boolean isOutsideMaxAge) {

    if (value == null) {
      return FieldStatus.NOT_FOUND;
    } else if (isOutsideMaxAge) {
      return FieldStatus.OUTSIDE_MAX_AGE;
    } else if (value.getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
      return FieldStatus.NULL_VALUE;
    }
    return FieldStatus.PRESENT;
  }

  /**
   * Determine if the feature data in the given feature row is outside maxAge. Data is outside
   * maxAge to be when the difference ingestion time set in feature row and the retrieval time set
   * in entity row exceeds FeatureTable max age.
   *
   * @param feature contains the ingestion timing and feature data.
   * @param entityTimestamp contains the retrieval timing of when features are pulled.
   * @param maxAge feature's max age.
   */
  private static boolean checkOutsideMaxAge(
      feast.storage.api.retriever.Feature feature, Timestamp entityTimestamp, Duration maxAge) {

    if (maxAge.equals(Duration.getDefaultInstance())) { // max age is not set
      return false;
    }

    long givenTimestamp = entityTimestamp.getSeconds();
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
   */
  private void populateHistogramMetrics(
      List<Map<String, ValueProto.Value>> entityRows, List<FeatureReferenceV2> featureReferences) {
    Metrics.requestEntityCountDistribution
        .labels(this.project)
        .observe(Double.valueOf(entityRows.size()));
    Metrics.requestFeatureCountDistribution
        .labels(this.project)
        .observe(Double.valueOf(featureReferences.size()));
  }

  /**
   * Populate count metrics that can be used for analysing online retrieval calls
   *
   * @param featureRef singe Feature Reference
   * @param featureVector Feature Vector built for this requested feature
   */
  private void populateCountMetrics(
      FeatureReferenceV2 featureRef,
      ServingAPIProto.GetOnlineFeaturesResponse.FeatureVectorOrBuilder featureVector) {
    String featureRefString = Feature.getFeatureReference(featureRef);
    featureVector
        .getStatusesList()
        .forEach(
            (status) -> {
              if (status == FieldStatus.NOT_FOUND) {
                Metrics.notFoundKeyCount.labels(this.project, featureRefString).inc();
              }
              if (status == FieldStatus.OUTSIDE_MAX_AGE) {
                Metrics.staleKeyCount.labels(this.project, featureRefString).inc();
              }
            });
  }

  private void populateFeatureCountMetrics(List<FeatureReferenceV2> featureReferences) {
    featureReferences.forEach(
        featureReference ->
            Metrics.requestFeatureCount
                .labels(project, Feature.getFeatureReference(featureReference))
                .inc());
  }
}
