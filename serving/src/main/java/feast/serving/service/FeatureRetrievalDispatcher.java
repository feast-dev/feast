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
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.config.AppConfig;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.serving.util.EntityMapBuilder;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
   * @param featureSpecs list of request.
   * @param timestampRange timestamp range of feature to be retrieved.
   * @return map of entityID and Entity instance.
   */
  public Map<String, Entity> dispatchFeatureRetrieval(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      TimestampRange timestampRange) {

    Map<String, List<FeatureSpec>> groupedFeatureSpecs = groupByStorage(featureSpecs);

    if (groupedFeatureSpecs.size() <= 1) {
      return runInCurrentThread(entityName, entityIds, timestampRange, groupedFeatureSpecs);
    } else {
      return runWithExecutorService(entityName, entityIds, timestampRange, groupedFeatureSpecs);
    }
  }

  /**
   * Execute request in current thread.
   *
   * @param entityName entity name of of the feature.
   * @param entityIds list of entity ID of the feature to be retrieved.
   * @param tsRange timestamp range of the feature to be retrieved.
   * @param groupedFeatureSpecs feature spec grouped by storage ID.
   * @return entity map containing the result of feature retrieval.
   */
  private Map<String, Entity> runInCurrentThread(
      String entityName,
      Collection<String> entityIds,
      TimestampRange tsRange,
      Map<String, List<FeatureSpec>> groupedFeatureSpecs) {
    try (Scope scope =
        tracer.buildSpan("FeatureRetrievalDispatcher-runInCurrentThread").startActive(true)) {

      String storageId = groupedFeatureSpecs.keySet().iterator().next();
      List<FeatureSpec> featureSpecs = groupedFeatureSpecs.get(storageId);
      FeatureStorage featureStorage = featureStorageRegistry.get(storageId);

      List<FeatureValue> featureValues;
      featureValues = featureStorage.getFeature(entityName, entityIds, featureSpecs, tsRange);

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
   * @param groupedFeatureSpec feature specs grouped by serving storage ID.
   * @return entity map containing result of feature retrieval.
   */
  private Map<String, Entity> runWithExecutorService(
      String entityName,
      Collection<String> entityIds,
      TimestampRange tsRange,
      Map<String, List<FeatureSpec>> groupedFeatureSpec) {
    try (Scope scope =
        tracer.buildSpan("FeatureRetrievalDispatcher-runWithExecutorService")
            .startActive(true)) {
      Span span = scope.span();
      List<ListenableFuture<Void>> futures = new ArrayList<>();
      EntityMapBuilder entityMapBuilder = new EntityMapBuilder();
      for (Map.Entry<String, List<FeatureSpec>> entry : groupedFeatureSpec.entrySet()) {
        FeatureStorage featureStorage = featureStorageRegistry.get(entry.getKey());
        List<FeatureSpec> featureSpecs = entry.getValue();
        futures.add(
            executorService.submit(
                () -> {
                  List<FeatureValue> featureValues =
                      featureStorage.getFeature(entityName, entityIds, featureSpecs, tsRange);
                  entityMapBuilder.addFeatureValueList(featureValues);
                  return null;
                }));
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
   * @param featureSpecs list of request.
   * @return request grouped by serving storage ID.
   */
  private Map<String, List<FeatureSpec>> groupByStorage(Collection<FeatureSpec> featureSpecs) {
    return featureSpecs
        .stream()
        .collect(groupingBy(featureSpec -> featureSpec.getDataStores().getServing().getId()));
  }
}
