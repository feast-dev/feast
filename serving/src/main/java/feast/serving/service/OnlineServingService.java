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

import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import feast.serving.ServingAPIProto.*;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.serving.specs.CachedSpecService;
import feast.serving.util.Metrics;
import feast.serving.util.RefUtil;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
      GetOnlineFeaturesResponse.Builder getOnlineFeaturesResponseBuilder =
          GetOnlineFeaturesResponse.newBuilder();
      List<FeatureSetRequest> featureSetRequests =
          specService.getFeatureSets(request.getFeaturesList());
      List<EntityRow> entityRows = request.getEntityRowsList();
      Map<EntityRow, Map<String, Value>> featureValuesMap =
          entityRows.stream()
              .collect(Collectors.toMap(row -> row, row -> Maps.newHashMap(row.getFieldsMap())));
      // Get all feature rows from the retriever. Each feature row list corresponds to a single
      // feature set request.
      List<List<FeatureRow>> featureRows =
          retriever.getOnlineFeatures(entityRows, featureSetRequests);

      // For each feature set request, read the feature rows returned by the retriever, and
      // populate the featureValuesMap with the feature values corresponding to that entity row.
      for (var fsIdx = 0; fsIdx < featureRows.size(); fsIdx++) {
        List<FeatureRow> featureRowsForFs = featureRows.get(fsIdx);
        FeatureSetRequest featureSetRequest = featureSetRequests.get(fsIdx);

        String project = featureSetRequest.getSpec().getProject();

        // In order to return values containing the same feature references provided by the user,
        // we reuse the feature references in the request as the keys in the featureValuesMap
        Map<String, FeatureReference> refsByName = featureSetRequest.getFeatureRefsByName();

        // Each feature row returned (per feature set request) corresponds to a given entity row.
        // For each feature row, update the featureValuesMap.
        for (var entityRowIdx = 0; entityRowIdx < entityRows.size(); entityRowIdx++) {
          FeatureRow featureRow = featureRowsForFs.get(entityRowIdx);
          EntityRow entityRow = entityRows.get(entityRowIdx);

          // If the row is stale, put an empty value into the featureValuesMap.
          if (isStale(featureSetRequest, entityRow, featureRow)) {
            featureSetRequest
                .getFeatureReferences()
                .parallelStream()
                .forEach(
                    ref -> {
                      populateStaleKeyCountMetrics(project, ref);
                      featureValuesMap
                          .get(entityRow)
                          .put(RefUtil.generateFeatureStringRef(ref), Value.newBuilder().build());
                    });

          } else {
            populateRequestCountMetrics(featureSetRequest);

            // Else populate the featureValueMap at this entityRow with the values in the feature
            // row.
            featureRow.getFieldsList().stream()
                .filter(field -> refsByName.containsKey(field.getName()))
                .forEach(
                    field -> {
                      FeatureReference ref = refsByName.get(field.getName());
                      String id = RefUtil.generateFeatureStringRef(ref);
                      featureValuesMap.get(entityRow).put(id, field.getValue());
                    });
          }
        }
      }

      List<FieldValues> fieldValues =
          featureValuesMap.values().stream()
              .map(valueMap -> FieldValues.newBuilder().putAllFields(valueMap).build())
              .collect(Collectors.toList());
      return getOnlineFeaturesResponseBuilder.addAllFieldValues(fieldValues).build();
    }
  }

  private void populateStaleKeyCountMetrics(String project, FeatureReference ref) {
    Metrics.staleKeyCount.labels(project, ref.getName()).inc();
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

  private boolean isStale(
      FeatureSetRequest featureSetRequest, EntityRow entityRow, FeatureRow featureRow) {
    Duration maxAge = featureSetRequest.getSpec().getMaxAge();
    if (maxAge.equals(Duration.getDefaultInstance())) {
      return false;
    }
    long givenTimestamp = entityRow.getEntityTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - featureRow.getEventTimestamp().getSeconds();
    return timeDifference > maxAge.getSeconds();
  }
}
