/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.service;

import com.google.common.collect.Sets;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.ServingAPIProto.QueryFeaturesRequest;
import feast.serving.ServingAPIProto.QueryFeaturesResponse;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.Collection;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Core service for feature retrieval. This class is responsible to retrieve featureSpec from core
 * API and coordinate feature retrieval from its associated storage.
 */
@Service
@Slf4j
public class FeastServing {

  public static final String SERVING_STORAGE_ID = "SERVING";

  private final SpecStorage specStorage;
  private final Tracer tracer;
  private final FeatureRetrievalDispatcher featureRetrievalDispatcher;

  @Autowired
  public FeastServing(
      FeatureRetrievalDispatcher featureRetrievalDispatcher,
      SpecStorage specStorage,
      Tracer tracer) {
    this.specStorage = specStorage;
    this.featureRetrievalDispatcher = featureRetrievalDispatcher;
    this.tracer = tracer;
  }

  /**
   * Query feature from feast storage.
   *
   * @param request feature query request.
   * @return response of the query containing the feature values.
   */
  public QueryFeaturesResponse queryFeatures(QueryFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("FeastServing-queryFeatures").startActive(true)) {
      Collection<FeatureSpec> featureSpecs = getFeatureSpecs(request.getFeatureIdList());

      scope.span().log("start retrieving all feature");
      Map<String, Entity> result =
          featureRetrievalDispatcher.dispatchFeatureRetrieval(
              request.getEntityName(),
              request.getEntityIdList(),
              featureSpecs);

      scope.span().log("finished retrieving all feature");

      // build response
      return QueryFeaturesResponse.newBuilder()
          .setEntityName(request.getEntityName())
          .putAllEntities(result)
          .build();
    }
  }

  /**
   * Attach request details with associated feature spec.
   *
   * @param featureIds collection of feature ID
   * @return collection of feature spec
   */
  private Collection<FeatureSpec> getFeatureSpecs(Collection<String> featureIds) {
    // dedup feature ID.
    Collection<String> featureIdSet = Sets.newHashSet(featureIds);

    Map<String, FeatureSpec> featureSpecMap = specStorage.getFeatureSpecs(featureIdSet);
    return featureSpecMap.values();
  }
}
