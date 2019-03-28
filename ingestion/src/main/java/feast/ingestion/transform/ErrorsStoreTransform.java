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

package feast.ingestion.transform;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.util.PathUtil;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.FeatureStoreWrite;
import feast.store.errors.FeatureErrorsFactory;
import feast.store.errors.json.JsonFileErrorsFactory;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Slf4j
public class ErrorsStoreTransform extends FeatureStoreWrite {

  private String workspace;
  private Specs specs;
  private List<FeatureErrorsFactory> errorsStoreFactories;

  @Inject
  public ErrorsStoreTransform(
      ImportJobPipelineOptions options, Specs specs,
      List<FeatureErrorsFactory> errorsStoreFactories) {
    this.workspace = options.getWorkspace();
    this.specs = specs;
    this.errorsStoreFactories = errorsStoreFactories;
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    StorageSpec errorsStoreSpec = specs.getErrorsStoreSpec();
    if (Strings.isNullOrEmpty(errorsStoreSpec.getType())) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(workspace), "workspace must be provided");
      Path workspaceErrorsPath = PathUtil.getPath(workspace).resolve("errors");
      errorsStoreSpec = StorageSpec.newBuilder()
          .setId("workspace/errors")
          .setType(JsonFileErrorsFactory.JSON_FILES_TYPE)
          .putOptions("path", workspaceErrorsPath.toString()).build();
    }
    input.apply("Write errors" + errorsStoreSpec.getType(),
        getErrorStore(errorsStoreSpec.getType()).create(errorsStoreSpec, specs));
    return PDone.in(input.getPipeline());
  }

  FeatureErrorsFactory getErrorStore(String type) {
    checkArgument(!type.isEmpty(), "Errors store type not provided");
    for (FeatureErrorsFactory errorsStoreFactory : errorsStoreFactories) {
      if (errorsStoreFactory.getType().equals(type)) {
        return errorsStoreFactory;
      }
    }
    throw new IllegalArgumentException("Errors store type not found");
  }
}
