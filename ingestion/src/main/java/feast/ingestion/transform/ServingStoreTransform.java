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
import feast.ingestion.metrics.FeastMetrics;
import feast.ingestion.model.Specs;
import feast.ingestion.values.PFeatureRows;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.serving.FeatureServingFactory;
import feast.types.FeatureRowExtendedProto;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Slf4j
public class ServingStoreTransform extends PTransform<PCollection<FeatureRowExtended>, PDone> {

  private List<FeatureServingFactory> stores;
  private Specs specs;

  @Inject
  public ServingStoreTransform(List<FeatureServingFactory> stores, Specs specs) {
    this.stores = stores;
    this.specs = specs;
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    StorageSpec storageSpec = specs.getServingStorageSpec();
    for (FeatureServingFactory store : stores) {
      if (store.getType().equals(storageSpec.getType())) {
        input.apply("metrics.store.lag", ParDo.of(FeastMetrics.lagUpdateDoFn()));
        input.apply("metrics.store.main", ParDo.of(FeastMetrics.incrDoFn("serving_stored")));
        return input.apply(store.create(storageSpec, specs));
      }
    }
    throw new IllegalArgumentException("No available serving store type matches " + storageSpec.getType());
  }
}
