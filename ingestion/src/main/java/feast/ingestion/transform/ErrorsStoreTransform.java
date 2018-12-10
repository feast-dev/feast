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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.inject.Inject;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobOptions;
import feast.ingestion.transform.FeatureIO.Write;
import feast.ingestion.transform.fn.LoggerDoFn;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.ErrorsStore;
import feast.storage.noop.NoOpIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import org.slf4j.event.Level;

@Slf4j
public class ErrorsStoreTransform extends FeatureIO.Write {
  public static final String STDERR_STORE_ID = "STDERR";
  public static final String STDOUT_STORE_ID = "STDOUT";

  private String errorsStoreId;
  private List<ErrorsStore> stores;
  private Specs specs;

  @Inject
  public ErrorsStoreTransform(ImportJobOptions options, List<ErrorsStore> stores, Specs specs) {
    this.errorsStoreId = options.getErrorsStoreId();
    this.stores = stores;
    this.specs = specs;
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    if (errorsStoreId == null) {
      log.warn("No errorsStoreId specified, errors will be discarded");
      return input.apply(new NoOpIO.Write());
    }

    if (errorsStoreId.equals(STDOUT_STORE_ID)) {
      input.apply("Log errors to STDOUT", ParDo.of(new LoggerDoFn(Level.INFO)));
    } else if (errorsStoreId.equals(STDERR_STORE_ID)) {
      input.apply("Log errors to STDERR", ParDo.of(new LoggerDoFn(Level.ERROR)));
    } else {
      StorageSpec storageSpec = specs.getStorageSpec(errorsStoreId);
      storageSpec =
          checkNotNull(
              storageSpec,
              String.format("errorsStoreId=%s not found in storage specs", errorsStoreId));
      Write write = null;
      for (ErrorsStore errorsStore : stores) {
        if (errorsStore.getType().equals(storageSpec.getType())) {
          write = errorsStore.create(storageSpec, specs);
        }
      }
      write =
          checkNotNull(
              write,
              "No errors storage factory found for errorsStoreId=%s with type=%s",
              errorsStoreId,
              storageSpec.getType());
      return input.apply(write);
    }
    return PDone.in(input.getPipeline());
  }
}
