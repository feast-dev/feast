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
import static feast.ingestion.util.JsonUtil.convertJsonStringToMap;

import com.google.inject.Inject;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobOptions;
import feast.ingestion.transform.FeatureIO.Write;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.ErrorsStore;
import feast.storage.noop.NoOpIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.util.Strings;

@Slf4j
public class ErrorsStoreTransform extends FeatureIO.Write {

  private String errorsStoreType;
  private StorageSpec errorsStoreSpec;
  private Specs specs;
  private List<ErrorsStore> errorsStores;

  @Inject
  public ErrorsStoreTransform(
      ImportJobOptions options, Specs specs, List<ErrorsStore> errorsStores) {
    this.specs = specs;
    this.errorsStores = errorsStores;
    this.errorsStoreType = options.getErrorsStoreType();

    if (!Strings.isEmpty(errorsStoreType)) {
      this.errorsStoreSpec =
          StorageSpec.newBuilder()
              .setType(errorsStoreType)
              .putAllOptions(convertJsonStringToMap(options.getErrorsStoreOptions()))
              .build();
    }
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    Write write;
    if (Strings.isEmpty(errorsStoreType)) {
      write = new NoOpIO.Write();
    } else {
      write = getErrorStore().create(this.errorsStoreSpec, specs);
    }
    input.apply("errors to " + String.valueOf(errorsStoreType), write);
    return PDone.in(input.getPipeline());
  }

  ErrorsStore getErrorStore() {
    checkArgument(!errorsStoreType.isEmpty(), "Errors store type not provided");
    for (ErrorsStore errorsStore : errorsStores) {
      if (errorsStore.getType().equals(errorsStoreType)) {
        return errorsStore;
      }
    }
    throw new IllegalArgumentException("Errors store type not found");
  }
}
