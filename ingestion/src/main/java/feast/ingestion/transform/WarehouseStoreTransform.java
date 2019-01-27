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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import feast.ingestion.metrics.FeastMetrics;
import feast.ingestion.model.Specs;
import feast.ingestion.values.PFeatureRows;
import feast.storage.FeatureWarehouseStoreFactory;

@Slf4j
public class WarehouseStoreTransform extends PTransform<PFeatureRows, PFeatureRows> {

  private List<FeatureWarehouseStoreFactory> stores;
  private Specs specs;

  @Inject
  public WarehouseStoreTransform(List<FeatureWarehouseStoreFactory> stores, Specs specs) {
    this.stores = stores;
    this.specs = specs;
  }

  @Override
  public PFeatureRows expand(PFeatureRows input) {
    PFeatureRows output =
        input.apply(
            "Split to warehouse stores",
            new SplitOutputByStore(
                stores,
                (featureSpec) -> featureSpec.getDataStores().getWarehouse().getId(),
                specs));
    output.getMain().apply("metrics.store.main", ParDo.of(FeastMetrics.incrDoFn("warehouse_stored")));
    output.getErrors().apply("metrics.store.errors", ParDo.of(FeastMetrics.incrDoFn("warehouse_errors")));
    return output;
  }
}
