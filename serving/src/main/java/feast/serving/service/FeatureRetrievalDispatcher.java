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

import com.google.common.collect.Lists;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.model.FeatureValue;
import feast.serving.util.EntityMapBuilder;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FeatureRetrievalDispatcher {

  private final FeatureStorageRegistry featureStorageRegistry;
  private final Tracer tracer;

  @Autowired
  public FeatureRetrievalDispatcher(
      FeatureStorageRegistry featureStorageRegistry,
      Tracer tracer) {
    this.featureStorageRegistry = featureStorageRegistry;
    this.tracer = tracer;
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
   * @return map of entityID and Entity instance.
   */
  public Map<String, Entity> dispatchFeatureRetrieval(
      String entityName, Collection<String> entityIds, Collection<FeatureSpec> featureSpecs) {

    return runInCurrentThread(entityName, entityIds, Lists.newArrayList(featureSpecs));
  }

  /**
   * Execute request in current thread.
   *
   * @param entityName entity name of of the feature.
   * @param entityIds list of entity ID of the feature to be retrieved.
   * @param featureSpecs list of feature specs
   * @return entity map containing the result of feature retrieval.
   */
  private Map<String, Entity> runInCurrentThread(
      String entityName,
      Collection<String> entityIds,
      List<FeatureSpec> featureSpecs) {
    try (Scope scope =
        tracer.buildSpan("FeatureRetrievalDispatcher-runInCurrentThread").startActive(true)) {

      String storageId = FeastServing.SERVING_STORAGE_ID;
      FeatureStorage featureStorage = featureStorageRegistry.get(storageId);

      List<FeatureValue> featureValues;
      featureValues = featureStorage.getFeature(entityName, entityIds, featureSpecs);

      EntityMapBuilder builder = new EntityMapBuilder();
      builder.addFeatureValueList(featureValues);
      return builder.toEntityMap();
    }
  }
}
