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

import feast.serving.ServingAPIProto.Entity;
import feast.serving.ServingAPIProto.QueryFeatures.Request;
import feast.serving.ServingAPIProto.QueryFeatures.Response;
import feast.serving.ServingAPIProto.RequestDetail;
import feast.serving.model.RequestDetailWithSpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  private final SpecStorage specStorage;
  private final FeatureStorageRegistry featureStorageRegistry;
  private final Tracer tracer;
  private final FeatureRetrievalDispatcher featureRetrievalDispatcher;

  @Autowired
  public FeastServing(
      FeatureRetrievalDispatcher featureRetrievalDispatcher,
      FeatureStorageRegistry featureStorageRegistry,
      SpecStorage specStorage,
      Tracer tracer) {
    this.specStorage = specStorage;
    this.featureStorageRegistry = featureStorageRegistry;
    this.featureRetrievalDispatcher = featureRetrievalDispatcher;
    this.tracer = tracer;
  }

  /**
   * Query feature from feast storage.
   *
   * @param request feature query request.
   * @return response of the query containing the feature values.
   */
  public Response queryFeatures(Request request) {
    try (Scope scope = tracer.buildSpan("FeastServing-queryFeatures").startActive(true)) {
      List<RequestDetailWithSpec> requestDetails = joinRequestDetailsWithFeatureSpec(request);

      // create connection to feature storage if necessary
      checkAndConnectFeatureStorage(
          requestDetails
              .stream()
              .map(r -> r.getFeatureSpec().getDataStores().getServing().getId())
              .collect(Collectors.toList()));

      scope.span().log("start retrieving all feature");
      Map<String, Entity> result =
          featureRetrievalDispatcher.dispatchFeatureRetrieval(
              request.getEntityName(),
              request.getEntityIdList(),
              requestDetails,
              request.getTimestampRange());

      scope.span().log("finished retrieving all feature");

      // build response
      return Response.newBuilder()
          .setEntityName(request.getEntityName())
          .putAllEntities(result)
          .build();
    }
  }

  /**
   * Check whether {@code featureStorageRegistry} has the connection to the associated feature
   * storage. If the connection doesn't exist then create one by first retrieve storage spec from
   * the {@code specStorage}.
   *
   * @param storageIds collection of storage ID to be checked.
   */
  private void checkAndConnectFeatureStorage(Collection<String> storageIds) {
    List<String> unknownStorageId = new ArrayList<>();
    for (String storageId : storageIds) {
      if (!featureStorageRegistry.hasStorageId(storageId)) {
        unknownStorageId.add(storageId);
      }
    }

    if (!unknownStorageId.isEmpty()) {
      Map<String, StorageSpec> storageSpecs = specStorage.getStorageSpecs(unknownStorageId);
      for (StorageSpec spec : storageSpecs.values()) {
        featureStorageRegistry.connect(spec);
      }
    }
  }

  /**
   * Attach request details with associated feature spec.
   *
   * @param request
   * @return
   */
  private List<RequestDetailWithSpec> joinRequestDetailsWithFeatureSpec(Request request) {
    // dedup feature ID.
    Collection<String> featuresId =
        request
            .getRequestDetailsList()
            .stream()
            .map(RequestDetail::getFeatureId)
            .collect(Collectors.toSet());

    Map<String, FeatureSpec> featureSpecMap = specStorage.getFeatureSpecs(featuresId);
    return request
        .getRequestDetailsList()
        .stream()
        .map(rd -> new RequestDetailWithSpec(rd, featureSpecMap.get(rd.getFeatureId())))
        .collect(Collectors.toList());
  }
}
