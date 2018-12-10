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

import static java.util.stream.Collectors.groupingBy;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.ServingAPIProto.RequestType;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.config.AppConfig;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.serving.model.Pair;
import feast.serving.model.RequestDetailWithSpec;
import feast.serving.util.EntityMapBuilder;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FeatureRetrievalDispatcher {

  private final FeatureStorageRegistry featureStorageRegistry;
  private final Tracer tracer;
  private final ListeningExecutorService executorService;
  private final int timeout;

  @Autowired
  public FeatureRetrievalDispatcher(
      FeatureStorageRegistry featureStorageRegistry,
      ListeningExecutorService executorService,
      AppConfig appConfig,
      Tracer tracer) {
    this.featureStorageRegistry = featureStorageRegistry;
    this.tracer = tracer;
    this.executorService = executorService;
    this.timeout = appConfig.getTimeout();
  }

  /**
   * Dispatch feature retrieval.
   *
   * <p>If request is small enough (only one request type and one source storage) it will be
   * executed in the current thread. Otherwise, the execution takes place in separate thread.
   *
   * @param entityName entity name of the feature.
   * @param entityIds list of entity ids.
   * @param requests list of request.
   * @param timestampRange timestamp range of feature to be retrieved.
   * @return map of entityID and Entity instance.
   */
  public Map<String, Entity> dispatchFeatureRetrieval(
      String entityName,
      List<String> entityIds,
      List<RequestDetailWithSpec> requests,
      TimestampRange timestampRange) {

    GroupedRequest groupedRequest = groupRequestByTypeAndStorage(requests);

    if (groupedRequest.getNbThreadRequired() <= 1) {
      return runInCurrentThread(
          entityName, entityIds, timestampRange, groupedRequest.getRequests());
    } else {
      return runWithExecutorService(
          entityName, entityIds, timestampRange, groupedRequest.getRequests());
    }
  }

  /**
   * Group request by its type and storage ID of the feature.
   *
   * @param requestDetailWithSpecs list of requests.
   * @return requests grouped by request type and storage ID.
   */
  private GroupedRequest groupRequestByTypeAndStorage(
      List<RequestDetailWithSpec> requestDetailWithSpecs) {
    Map<RequestType, List<RequestDetailWithSpec>> groupedByRequestType =
        requestDetailWithSpecs.stream()
            .collect(groupingBy(r -> r.getRequestDetail().getType()));

    int nbThreadRequired = 0;
    Map<RequestType, Map<String, List<RequestDetailWithSpec>>> groupedByTypeAndStorageId =
        new HashMap<>();
    for (Map.Entry<RequestType, List<RequestDetailWithSpec>> requestEntry :
        groupedByRequestType.entrySet()) {
      RequestType requestType = requestEntry.getKey();
      List<RequestDetailWithSpec> requestDetails = requestEntry.getValue();

      Map<String, List<RequestDetailWithSpec>> groupedByStorageId =
          groupRequestByStorage(requestDetails);
      groupedByTypeAndStorageId.put(requestType, groupedByStorageId);

      nbThreadRequired += groupedByStorageId.size();
    }
    return new GroupedRequest(groupedByTypeAndStorageId, nbThreadRequired);
  }

  /**
   * Execute request in current thread.
   *
   * @param entityName entity name of of the feature.
   * @param entityIds list of entity ID of the feature to be retrieved.
   * @param tsRange timestamp range of the feature to be retrieved.
   * @param groupedRequest request grouped by type and storage ID.
   * @return entity map containing the result of feature retrieval.
   */
  private Map<String, Entity> runInCurrentThread(
      String entityName,
      List<String> entityIds,
      TimestampRange tsRange,
      Map<RequestType, Map<String, List<RequestDetailWithSpec>>> groupedRequest) {
    try (Scope scope = tracer.buildSpan("FeatureRetrievalDispatcher-runInCurrentThread")
        .startActive(true)) {
      Span span = scope.span();
      if (groupedRequest.size() > 1) {
        throw new IllegalArgumentException(
            "runInCurrentThread required more than one thread to run");
      }

      RequestType requestType = groupedRequest.keySet().iterator().next();
      Map<String, List<RequestDetailWithSpec>> request = groupedRequest.get(requestType);

      if (request.size() > 1) {
        throw new IllegalArgumentException(
            "runInCurrentThread required more than one thread to run");
      }

      String storageId = request.keySet().iterator().next();
      List<RequestDetailWithSpec> requestDetailWithSpecs = request.get(storageId);
      FeatureStorage featureStorage = featureStorageRegistry.get(storageId);

      List<FeatureValue> featureValues;
      if (RequestType.LAST == requestType) {
        span.setTag("request-type", "last");
        List<FeatureSpec> featureSpecs =
            requestDetailWithSpecs
                .stream()
                .map(RequestDetailWithSpec::getFeatureSpec)
                .collect(Collectors.toList());
        featureValues = featureStorage.getCurrentFeatures(entityName, entityIds, featureSpecs);
      } else {
        span.setTag("request-type", "list");
        List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs =
            requestDetailWithSpecs
                .stream()
                .map(r -> new Pair<>(r.getFeatureSpec(), r.getRequestDetail().getLimit()))
                .collect(Collectors.toList());
        featureValues =
            featureStorage.getNLatestFeaturesWithinTimestampRange(
                entityName, entityIds, featureSpecLimitPairs, tsRange);
      }

      EntityMapBuilder builder = new EntityMapBuilder();
      builder.addFeatureValueList(featureValues);
      return builder.toEntityMap();
    }
  }

  /**
   * Execute feature retrieval in parallel using executor service.
   *
   * @param entityName entity name of the feature.
   * @param entityIds list of entity ID.
   * @param tsRange timestamp range of the feature.
   * @param groupedRequest request grouped by type and serving storage ID.
   * @return entity map containing result of feature retrieval.
   */
  private Map<String, Entity> runWithExecutorService(
      String entityName,
      List<String> entityIds,
      TimestampRange tsRange,
      Map<RequestType, Map<String, List<RequestDetailWithSpec>>> groupedRequest) {
    try (Scope scope = tracer.buildSpan("FeatureRetrievalDispatcher-runWithExecutorService")
        .startActive(true)) {
      Span span = scope.span();
      List<ListenableFuture<Void>> futures = new ArrayList<>();
      EntityMapBuilder entityMapBuilder = new EntityMapBuilder();
      for (Map.Entry<RequestType, Map<String, List<RequestDetailWithSpec>>> requestEntryPerType :
          groupedRequest.entrySet()) {
        RequestType requestType = requestEntryPerType.getKey();
        for (Map.Entry<String, List<RequestDetailWithSpec>> requestEntryPerStorage :
            requestEntryPerType.getValue().entrySet()) {
          List<RequestDetailWithSpec> requests = requestEntryPerStorage.getValue();
          FeatureStorage featureStorage = featureStorageRegistry
              .get(requestEntryPerStorage.getKey());
          if (requestType == RequestType.LAST) {
            List<FeatureSpec> featureSpecs =
                requests
                    .stream()
                    .map(RequestDetailWithSpec::getFeatureSpec)
                    .collect(Collectors.toList());

            futures.add(
                executorService.submit(
                    () -> {
                      List<FeatureValue> featureValues =
                          featureStorage.getCurrentFeatures(entityName, entityIds, featureSpecs);
                      entityMapBuilder.addFeatureValueList(featureValues);
                      return null;
                    }));
          } else {
            List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs =
                requests
                    .stream()
                    .map(r -> new Pair<>(r.getFeatureSpec(), r.getRequestDetail().getLimit()))
                    .collect(Collectors.toList());

            futures.add(
                executorService.submit(
                    () -> {
                      List<FeatureValue> featureValues =
                          featureStorage.getNLatestFeaturesWithinTimestampRange(
                              entityName, entityIds, featureSpecLimitPairs, tsRange);
                      entityMapBuilder.addFeatureValueList(featureValues);
                      return null;
                    }));
          }
        }
      }
      span.log("submitted all task");
      ListenableFuture<List<Void>> combined = Futures.allAsList(futures);
      try {
        combined.get(timeout, TimeUnit.SECONDS);
        span.log("completed getting all result");
        return entityMapBuilder.toEntityMap();
      } catch (InterruptedException e) {
        log.error("Interrupted exception while processing futures", e);
        throw new FeatureRetrievalException("Interrupted exception while processing futures", e);
      } catch (ExecutionException e) {
        log.error("Execution exception while processing futures", e);
        throw new FeatureRetrievalException("Execution exception while processing futures", e);
      } catch (TimeoutException e) {
        log.error("Timeout exception while processing futures", e);
        throw new FeatureRetrievalException(
            "Timeout exception exception while processing futures", e);
      }
    }
  }

  /**
   * Group request by its serving storage ID.
   *
   * @param requestDetailWithSpecs list of request.
   * @return request grouped by serving storage ID.
   */
  private Map<String, List<RequestDetailWithSpec>> groupRequestByStorage(
      List<RequestDetailWithSpec> requestDetailWithSpecs) {
    return requestDetailWithSpecs
        .stream()
        .collect(groupingBy(r -> r.getFeatureSpec().getDataStores().getServing().getId()));
  }

  @AllArgsConstructor
  @Getter
  private static class GroupedRequest {

    /**
     * request grouped by type and its serving storage ID
     */
    private final Map<RequestType, Map<String, List<RequestDetailWithSpec>>> requests;
    /**
     * number of thread required to execute all request in parallel
     */
    private final int nbThreadRequired;
  }
}
