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
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.store.errors.FeatureErrorsFactory;
import feast.store.errors.FeatureErrorsFactoryService;
import feast.store.serving.FeatureServingFactory;
import feast.store.serving.FeatureServingFactoryService;
import feast.store.warehouse.FeatureWarehouseFactory;
import feast.store.warehouse.FeatureWarehouseFactoryService;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * An ImportJobModule is a Guice module for creating dependency injection bindings.
 */
public class ImportJobModule extends AbstractModule {

  private final ImportJobPipelineOptions options;
  private ImportJobSpecs importJobSpecs;

  public ImportJobModule(ImportJobPipelineOptions options, ImportJobSpecs importJobSpecs) {
    this.options = options;
    this.importJobSpecs = importJobSpecs;
  }

  @Override
  protected void configure() {
    bind(ImportJobPipelineOptions.class).toInstance(options);
    bind(PipelineOptions.class).toInstance(options);
    bind(ImportJobSpecs.class).toInstance(importJobSpecs);
  }

  @Provides
  @Singleton
  Specs provideSpecs() {
    return Specs.of(options.getJobName(), importJobSpecs);
  }

  @Provides
  @Singleton
  List<FeatureWarehouseFactory> provideWarehouseStores() {
    return FeatureWarehouseFactoryService.getAll();
  }

  @Provides
  @Singleton
  List<FeatureServingFactory> provideServingStores() {
    return FeatureServingFactoryService.getAll();
  }

  @Provides
  @Singleton
  List<FeatureErrorsFactory> provideErrorsStores() {
    return FeatureErrorsFactoryService.getAll();
  }
}
