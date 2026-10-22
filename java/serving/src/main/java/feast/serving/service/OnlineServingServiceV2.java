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
import feast.serving.connectors.Feature;
import feast.serving.connectors.OnlineRetriever;
import feast.serving.registry.RegistryRepository;
import feast.serving.util.Metrics;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

// TODO(adchia): Delegate to FeatureStore class that acts as a fat client
public class OnlineServingServiceV2 implements ServingServiceV2 {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingServiceV2.class);
  private final Optional<Tracer> tracerOptional;
  private final OnlineRetriever retriever;
  private final RegistryRepository registryRepository;
  private final OnlineTransformationService onlineTransformationService;
  private final String project;

  public static final String DUMMY_ENTITY_ID = "__dummy_id";
  public static final String DUMMY_ENTITY_VAL = "";
  public static final ValueProto.Value DUMMY_ENTITY_VALUE =
      ValueProto.Value.newBuilder().setStringVal(DUMMY_ENTITY_VAL).build();

  public OnlineServingServiceV2(
      OnlineRetriever retriever,
      RegistryRepository registryRepository,
      OnlineTransformationService onlineTransformationService,
      String project,
      Optional<Tracer> tracerOptional) {
    this.retriever = retriever;
    this.registryRepository = registryRepository;
    this.onlineTransformationService = onlineTransformationService;
    this.project = project;
    this.tracerOptional = tracerOptional;
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
    // Pair from extractRequestDataFeatureNamesAndOnDemandFeatureSources.
    // Currently, we can retrieve context variables directly from GetOnlineFeaturesRequest.
    List<FeatureReferenceV2> onDemandFeatureSources =
        this.onlineTransformationService.extractOnDemandFeaturesDependencies(
            onDemandFeatureReferences);

    // Add on demand feature sources to list of feature references to retrieve.
    for (FeatureReferenceV2 onDemandFeatureSource : onDemandFeatureSources) {
      if (!retrievedFeatureReferences.contains(onDemandFeatureSource)) {
        retrievedFeatureReferences.add(onDemandFeatureSource);
      }
    }

    List<Map<String, ValueProto.Value>> entityRows = getEntityRows(request);

    Span storageRetrievalSpan =
        tracerOptional.map(tracer -> tracer.buildSpan("storageRetrieval").start()).orElse(null);
    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.setTag("entities", entityRows.size());
      storageRetrievalSpan.setTag("features", retrievedFeatureReferences.size());
    }

    List<List<Feature>> features = retrieveFeatures(retrievedFeatureReferences, entityRows);

    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.finish();
    }

    Span postProcessingSpan =
        tracerOptional.map(tracer -> tracer.buildSpan("postProcessing").start()).orElse(null);

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
        Feature feature = features.get(rowIdx).get(featureIdx);
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
                            .map(FeatureUtil::getFeatureReference)
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
          onDemandFeatureSources,
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
          .map(FeatureUtil::parseFeatureReference)
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

  private List<List<Feature>> retrieveFeatures(
      List<FeatureReferenceV2> featureReferences, List<Map<String, ValueProto.Value>> entityRows) {
    // Prepare feature reference to index mapping. This mapping will be used to arrange the
    // retrieved features to the same order as in the input.
    if (featureReferences.isEmpty()) {
      throw new RuntimeException("Requested features list must not be empty.");
    }
    Map<FeatureReferenceV2, Integer> featureReferenceToIndexMap =
        new HashMap<>(featureReferences.size());
    for (int i = 0; i < featureReferences.size(); i++) {
      FeatureReferenceV2 featureReference = featureReferences.get(i);
      if (featureReferenceToIndexMap.containsKey(featureReference)) {
        throw new RuntimeException(
            String.format(
                "Found duplicate features %s:%s.",
                featureReference.getFeatureViewName(), featureReference.getFeatureName()));
      }
      featureReferenceToIndexMap.put(featureReference, i);
    }

    // Create placeholders for retrieved features.
    List<List<Feature>> features = new ArrayList<>(entityRows.size());
    for (int i = 0; i < entityRows.size(); i++) {
      List<Feature> featuresPerEntity = new ArrayList<>(featureReferences.size());
      for (int j = 0; j < featureReferences.size(); j++) {
        featuresPerEntity.add(null);
      }
      features.add(featuresPerEntity);
    }

    // Group feature references by join keys.
    Map<String, List<FeatureReferenceV2>> groupNameToFeatureReferencesMap =
        featureReferences.stream()
            .collect(
                Collectors.groupingBy(
                    featureReference ->
                        this.registryRepository.getEntitiesList(featureReference).stream()
                            .map(this.registryRepository::getEntityJoinKey)
                            .sorted()
                            .collect(Collectors.joining(","))));

    // Retrieve features one group at a time.
    for (List<FeatureReferenceV2> featureReferencesPerGroup :
        groupNameToFeatureReferencesMap.values()) {
      List<String> entityNames =
          this.registryRepository.getEntitiesList(featureReferencesPerGroup.get(0));
      List<Map<String, ValueProto.Value>> entityRowsPerGroup = new ArrayList<>(entityRows.size());
      for (Map<String, ValueProto.Value> entityRow : entityRows) {
        Map<String, ValueProto.Value> entityRowPerGroup = new HashMap<>();
        entityNames.stream()
            .map(this.registryRepository::getEntityJoinKey)
            .forEach(
                joinKey -> {
                  if (joinKey.equals(DUMMY_ENTITY_ID)) {
                    entityRowPerGroup.put(joinKey, DUMMY_ENTITY_VALUE);
                  } else {
                    ValueProto.Value value = entityRow.get(joinKey);
                    if (value != null) {
                      entityRowPerGroup.put(joinKey, value);
                    }
                  }
                });
        entityRowsPerGroup.add(entityRowPerGroup);
      }
      List<List<Feature>> featuresPerGroup =
          retriever.getOnlineFeatures(entityRowsPerGroup, featureReferencesPerGroup, entityNames);
      for (int i = 0; i < featuresPerGroup.size(); i++) {
        for (int j = 0; j < featureReferencesPerGroup.size(); j++) {
          int k = featureReferenceToIndexMap.get(featureReferencesPerGroup.get(j));
          features.get(i).set(k, featuresPerGroup.get(i).get(j));
        }
      }
    }

    return features;
  }

  private void populateOnDemandFeatures(
      List<FeatureReferenceV2> onDemandFeatureReferences,
      List<FeatureReferenceV2> onDemandFeatureSources,
      List<FeatureReferenceV2> retrievedFeatureReferences,
      ServingAPIProto.GetOnlineFeaturesRequest request,
      List<List<Feature>> features,
      ServingAPIProto.GetOnlineFeaturesResponse.Builder responseBuilder) {

    List<Pair<String, List<ValueProto.Value>>> onDemandContext =
        request.getRequestContextMap().entrySet().stream()
            .map(e -> Pair.of(e.getKey(), e.getValue().getValList()))
            .collect(Collectors.toList());

    for (int featureIdx = 0; featureIdx < retrievedFeatureReferences.size(); featureIdx++) {
      FeatureReferenceV2 featureReference = retrievedFeatureReferences.get(featureIdx);

      if (!onDemandFeatureSources.contains(featureReference)) {
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
            .map(r -> FeatureUtil.getFeatureReference(r))
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
      Feature feature, Timestamp entityTimestamp, Duration maxAge) {

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
    String featureRefString = FeatureUtil.getFeatureReference(featureRef);
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
                .labels(project, FeatureUtil.getFeatureReference(featureReference))
                .inc());
  }
}
