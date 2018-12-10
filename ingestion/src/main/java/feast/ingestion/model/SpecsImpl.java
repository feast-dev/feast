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

package feast.ingestion.model;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import feast.ingestion.service.SpecService;
import feast.ingestion.service.SpecService.Builder;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.StorageSpecProto.StorageSpec;

public class SpecsImpl implements Specs {
  private String jobName;
  private ImportSpec importSpec;
  private Builder specServiceBuilder;
  private transient SpecService specService;

  @Inject
  public SpecsImpl(
      PipelineOptions options, ImportSpec importSpec, SpecService.Builder specServiceBuilder) {
    if (options != null) {
      this.jobName = options.getJobName();
    }
    this.importSpec = importSpec; 
    this.specServiceBuilder = specServiceBuilder;
  }

  private SpecService getSpecService() {
    if (specService == null) {
      specService = specServiceBuilder.build();
    }
    return specService;
  }

  public FeatureSpec getFeatureSpec(String featureId) {
    FeatureSpec spec =
        getSpecService().getFeatureSpecs(Collections.singleton(featureId)).get(featureId);
    Preconditions.checkNotNull(spec, "Spec not found for feature " + featureId);
    return spec;
  }

  public EntitySpec getEntitySpec(String entityName) {
    EntitySpec spec =
        getSpecService().getEntitySpecs(Collections.singleton(entityName)).get(entityName);
    Preconditions.checkNotNull(spec, "Spec not found for entity " + entityName);
    return spec;
  }

  public ImportSpec getImportSpec() {
    Preconditions.checkNotNull(importSpec, "Import spec not found");
    return importSpec;
  }

  public Map<String, StorageSpec> getStorageSpecs() {
    return getSpecService().getAllStorageSpecs();
  }

  @Override
  public StorageSpec getStorageSpec(String storeId) {
    StorageSpec spec =
        getSpecService().getStorageSpecs(Collections.singleton(storeId)).get(storeId);
    Preconditions.checkNotNull(spec, "Spec not found for storeId " + storeId);
    return spec;
  }

  @Override
  public String getJobName() {
    return jobName;
  }

  @Override
  public List<FeatureSpec> getFeatureSpecByServingStoreId(String storeId) {
    List<FeatureSpec> featureSpecs = new ArrayList<>();
    for (FeatureSpec featureSpec : getSpecService().getAllFeatureSpecs().values()) {
      if (featureSpec.getDataStores().getServing().getId().equals(storeId)) {
        featureSpecs.add(featureSpec);
      }
    }
    return featureSpecs;
  }
}
