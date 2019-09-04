// /*
//  * Copyright 2018 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     https://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  *
//  */
//
// package feast.ingestion.transform;
//
// import com.google.common.base.Preconditions;
// import com.google.inject.Inject;
// import feast.ingestion.metrics.FeastMetrics;
// import feast.ingestion.model.Specs;
// import feast.specs.StorageSpecProto.StorageSpec;
// import feast.store.FeatureStoreFactory;
// import feast.store.FeatureStoreWrite;
// import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
// import java.util.List;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.beam.sdk.transforms.PTransform;
// import org.apache.beam.sdk.transforms.ParDo;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PDone;
//
// @Slf4j
// public class BaseStoreTransform extends PTransform<PCollection<FeatureRowExtended>, PDone> {
//
//   private List<? extends FeatureStoreFactory> stores;
//   private StorageSpec storageSpec;
//   private Specs specs;
//
//   @Inject
//   public BaseStoreTransform(List<? extends FeatureStoreFactory> stores, StorageSpec storageSpec,
//       Specs specs) {
//     this.stores = stores;
//     this.storageSpec = storageSpec;
//     this.specs = specs;
//   }
//
//   @Override
//   public PDone expand(PCollection<FeatureRowExtended> input) {
//     FeatureStoreWrite write = null;
//     for (FeatureStoreFactory factory : stores) {
//       log.info("Checking factory {} vs storageSpec {}", factory.getType(), storageSpec);
//       if (factory.getType().equals(storageSpec.getType())) {
//         write = factory.create(storageSpec, specs);
//       }
//     }
//     Preconditions.checkNotNull(write,
//         "Store %s with type %s not supported",
//         storageSpec.getId(),
//         storageSpec.getType());
//
//     input.apply("metrics.store." + storageSpec.getId(),
//         ParDo.of(FeastMetrics.incrDoFn("stored")));
//
//     return input.apply(
//         String.format("Write %s %s", storageSpec.getId(), storageSpec.getType()),
//         write);
//   }
// }
