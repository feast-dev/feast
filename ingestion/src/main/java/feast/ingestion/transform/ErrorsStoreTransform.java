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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.util.PathUtil;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.errors.FeatureErrorsFactory;
import feast.store.errors.json.JsonFileErrorsFactory;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class ErrorsStoreTransform extends BaseStoreTransform {

  @Inject
  public ErrorsStoreTransform(
      List<FeatureErrorsFactory> errorsStoreFactories,
      Specs specs,
      ImportJobPipelineOptions options) {
    super(errorsStoreFactories, getErrorStoreSpec(specs, options), specs);
  }

  public static StorageSpec getErrorStoreSpec(Specs specs, ImportJobPipelineOptions options) {
    String workspace = options.getWorkspace();
    StorageSpec errorsStoreSpec = specs.getErrorsStoreSpec();
    if (Strings.isNullOrEmpty(errorsStoreSpec.getType())) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(workspace), "Workspace must be provided");
      Path workspaceErrorsPath =
          PathUtil.getPath(workspace)
              .resolve(
                  "errors-" + new Instant().toString(ISODateTimeFormat.dateHourMinuteSecondMillis()));
      try {
        Files.createDirectory(workspaceErrorsPath);
      } catch (IOException e) {
        log.error("Could not intialise workspace errors directory {}", workspaceErrorsPath);
        throw new RuntimeException(e);
      }
      errorsStoreSpec =
          StorageSpec.newBuilder()
              .setId("ERRORS")
              .setType(JsonFileErrorsFactory.JSON_FILES_TYPE)
              .putOptions("path", workspaceErrorsPath.toString())
              .build();
    }
    return errorsStoreSpec;
  }
}
