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

package feast.ingestion.config;

import com.google.inject.Inject;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.function.Supplier;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.util.ProtoUtil;
import feast.specs.ImportSpecProto.ImportSpec;

public class ImportSpecSupplier implements Supplier<ImportSpec> {

  private ImportJobPipelineOptions options;
  private ImportSpec importSpec;

  @Inject
  public ImportSpecSupplier(ImportJobPipelineOptions options) {
    this.options = options;
  }

  private ImportSpec create() {
    try {
      if (options.getImportSpecYamlFile() != null) {
        return ProtoUtil.decodeProtoYamlFile(
            Paths.get(options.getImportSpecYamlFile()), ImportSpec.getDefaultInstance());
      } else if (options.getImportSpecBase64() != null) {
        byte[] bytes = Base64.getDecoder().decode(options.getImportSpecBase64());
        return ImportSpec.parseFrom(bytes);
      } else {
        return ImportSpec.newBuilder().build();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ImportSpec get() {
    if (importSpec == null) {
      importSpec = create();
    }
    return importSpec;
  }
}
