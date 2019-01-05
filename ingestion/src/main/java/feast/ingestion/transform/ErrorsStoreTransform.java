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

import com.google.inject.Inject;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobOptions;
import feast.ingestion.transform.FeatureIO.Write;
import feast.ingestion.transform.fn.LoggerDoFn;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.ErrorsStore;
import feast.storage.noop.NoOpIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.event.Level;

import javax.annotation.Nullable;

import static feast.ingestion.util.JsonUtil.convertJsonStringToMap;

@Slf4j
public class ErrorsStoreTransform extends FeatureIO.Write {
  public static final String ERRORS_STORE_STDERR = "stderr";
  public static final String ERRORS_STORE_STDOUT = "stdout";
  public static final String ERRORS_STORE_JSON = "file.json";

  private String errorsStoreType;
  private StorageSpec errorsStoreSpec;
  private ErrorsStore errorsStore;
  private Specs specs;

  @Inject
  public ErrorsStoreTransform(
      ImportJobOptions options, Specs specs, @Nullable ErrorsStore errorsStore) {
    this.specs = specs;
    this.errorsStoreType = options.getErrorsStoreType();
    this.errorsStore = errorsStore;
    StorageSpec.Builder errorsStoreBuilder = StorageSpec.newBuilder().setType(errorsStoreType);

    switch (errorsStoreType) {
      case ERRORS_STORE_JSON:
        errorsStoreBuilder.putAllOptions(convertJsonStringToMap(options.getErrorsStoreOptions()));
      default:
        this.errorsStoreSpec = errorsStoreBuilder.build();
    }
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    switch (errorsStoreType) {
      case ERRORS_STORE_STDOUT:
        input.apply("Log errors to STDOUT", ParDo.of(new LoggerDoFn(Level.INFO)));
        break;
      case ERRORS_STORE_STDERR:
        input.apply("Log errors to STDERR", ParDo.of(new LoggerDoFn(Level.ERROR)));
        break;
      default:
        if (errorsStore == null) {
          log.warn("No valid errors store specified, errors will be discarded");
          return input.apply(new NoOpIO.Write());
        }
        Write write = errorsStore.create(this.errorsStoreSpec, specs);
        return input.apply(write);
    }
    return PDone.in(input.getPipeline());
  }
}
