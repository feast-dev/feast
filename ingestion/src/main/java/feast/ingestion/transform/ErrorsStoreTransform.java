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
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.FeatureStoreWrite;
import feast.store.NoOpIO;
import feast.store.errors.FeatureErrorsFactory;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.util.Strings;

@Slf4j
public class ErrorsStoreTransform extends FeatureStoreWrite {

  private String errorsStoreType;
  private StorageSpec errorsStoreSpec;
  private Specs specs;
  private List<FeatureErrorsFactory> errorsStoreFactories;

  @Inject
  public ErrorsStoreTransform(
      ImportJobPipelineOptions options, Specs specs,
      List<FeatureErrorsFactory> errorsStoreFactories) {
    this.specs = specs;
    this.errorsStoreFactories = errorsStoreFactories;
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
    FeatureStoreWrite write;
    if (Strings.isEmpty(errorsStoreType)) {
      write = new NoOpIO.Write();
    } else {
      write = getErrorStore().create(this.errorsStoreSpec, specs);
    }
    input.apply("errors to " + String.valueOf(errorsStoreType), write);
    return PDone.in(input.getPipeline());
  }

  FeatureErrorsFactory getErrorStore() {
    checkArgument(!errorsStoreType.isEmpty(), "Errors store type not provided");
    for (FeatureErrorsFactory errorsStoreFactory : errorsStoreFactories) {
      if (errorsStoreFactory.getType().equals(errorsStoreType)) {
        return errorsStoreFactory;
      }
    }
    throw new IllegalArgumentException("Errors store type not found");
  }
}
