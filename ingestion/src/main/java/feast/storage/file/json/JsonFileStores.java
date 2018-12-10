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

package feast.storage.file.json;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import lombok.AllArgsConstructor;
import feast.ingestion.model.Specs;
import feast.ingestion.transform.FeatureIO.Read;
import feast.ingestion.transform.FeatureIO.Write;
import feast.options.OptionsParser;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.ErrorsStore;
import feast.storage.FeatureSource;
import feast.storage.ServingStore;
import feast.storage.WarehouseStore;
import feast.storage.file.FileStoreOptions;

public class JsonFileStores {
  private static final String JSON_FILES_TYPE = "file.json";

  public static class JsonFileFeatureSource implements FeatureSource {

    @Override
    public Read create(ImportSpec importSpec) {
      checkArgument(importSpec.getType().equals(getType()));
      JsonFileSourceOptions options =
          OptionsParser.parse(importSpec.getOptionsMap(), JsonFileSourceOptions.class);
      return new JsonFileFeatureIO.Read(options);
    }

    @Override
    public String getType() {
      return JSON_FILES_TYPE;
    }
  }

  @AutoService(WarehouseStore.class)
  @AllArgsConstructor
  public static class JsonFileWarehouseStore implements WarehouseStore {

    @Override
    public Write create(StorageSpec storageSpec, Specs specs) {
      FileStoreOptions options =
          OptionsParser.parse(storageSpec.getOptionsMap(), FileStoreOptions.class);
      options.jobName = specs.getJobName();
      return JsonFileFeatureIO.writeRow(options);
    }

    @Override
    public String getType() {
      return JSON_FILES_TYPE;
    }
  }

  @AutoService(ServingStore.class)
  @AllArgsConstructor
  public static class JsonFileServingStore implements ServingStore {

    @Override
    public Write create(StorageSpec storageSpec, Specs specs) {
      FileStoreOptions options =
          OptionsParser.parse(storageSpec.getOptionsMap(), FileStoreOptions.class);
      options.jobName = specs.getJobName();
      return JsonFileFeatureIO.writeRow(options);
    }

    @Override
    public String getType() {
      return JSON_FILES_TYPE;
    }
  }

  @AutoService(ErrorsStore.class)
  @AllArgsConstructor
  public static class JsonFileErrorsStore implements ErrorsStore {

    @Override
    public Write create(StorageSpec storageSpec, Specs specs) {
      FileStoreOptions options =
          OptionsParser.parse(storageSpec.getOptionsMap(), FileStoreOptions.class);
      options.jobName = specs.getJobName();
      return JsonFileFeatureIO.writeRowExtended(options);
    }

    @Override
    public String getType() {
      return JSON_FILES_TYPE;
    }
  }
}
