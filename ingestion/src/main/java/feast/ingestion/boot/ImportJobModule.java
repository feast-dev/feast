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

package feast.ingestion.boot;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.service.CoreSpecService;
import feast.ingestion.service.FileSpecService;
import feast.ingestion.service.SpecService;
import feast.ingestion.service.SpecService.Builder;
import feast.ingestion.service.SpecService.UnsupportedBuilder;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.storage.FeatureErrorsStoreFactory;
import feast.storage.FeatureServingStoreFactory;
import feast.storage.FeatureWarehouseStoreFactory;
import feast.storage.service.FeatureErrorsStoreFactoryService;
import feast.storage.service.FeatureServingStoreFactoryService;
import feast.storage.service.FeatureWarehouseStoreFactoryService;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * An ImportJobModule is a Guice module for creating dependency injection bindings.
 */
public class ImportJobModule extends AbstractModule {

  private final ImportJobPipelineOptions options;
  private ImportSpec importSpec;

  public ImportJobModule(ImportJobPipelineOptions options, ImportSpec importSpec) {
    this.options = options;
    this.importSpec = importSpec;
  }

  @Override
  protected void configure() {
    bind(ImportJobPipelineOptions.class).toInstance(options);
    bind(PipelineOptions.class).toInstance(options);
    bind(ImportSpec.class).toInstance(importSpec);
  }

  @Provides
  @Singleton
  Builder provideSpecService(ImportJobPipelineOptions options) {
    if (options.getCoreApiUri() != null) {
      return new CoreSpecService.Builder(options.getCoreApiUri());
    } else if (options.getCoreApiSpecPath() != null) {
      return new FileSpecService.Builder(options.getCoreApiSpecPath());
    } else {
      return new UnsupportedBuilder(
          "Cannot initialise spec service as coreApiHost or specPath was not set.");
    }
  }

  @Provides
  @Singleton
  Specs provideSpecs(SpecService.Builder specService) {
    return Specs.of(options.getJobName(), importSpec, specService.build());
  }

  @Provides
  @Singleton
  List<FeatureWarehouseStoreFactory> provideWarehouseStores() {
    return FeatureWarehouseStoreFactoryService.getAll();
  }

  @Provides
  @Singleton
  List<FeatureServingStoreFactory> provideServingStores() {
    return FeatureServingStoreFactoryService.getAll();
  }

  @Provides
  @Singleton
  List<FeatureErrorsStoreFactory> provideErrorsStores() {
    return FeatureErrorsStoreFactoryService.getAll();
  }
}
