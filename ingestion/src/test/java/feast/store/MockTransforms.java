// /*
//  * Copyright 2019 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  *
//  */
//
// package feast.store;
//
// import com.google.common.collect.Lists;
// import feast.ingestion.transform.fn.Identity;
// import feast.specs.StorageSpecProto.StorageSpec;
// import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
// import java.util.List;
// import lombok.Getter;
// import org.apache.beam.sdk.transforms.ParDo;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PDone;
//
// public class MockTransforms {
//
//   @Getter
//   public static class Write extends FeatureStoreWrite {
//
//     List<PCollection<FeatureRowExtended>> inputs = Lists.newArrayList();
//     private StorageSpec spec;
//
//     public Write() {
//     }
//
//     public Write(StorageSpec spec) {
//       this.spec = spec;
//     }
//
//     /**
//      * Keep input around for testing and apply identity because PTransforms have to do something, or
//      * the pipeline hangs at execution time
//      */
//     @Override
//     public PDone expand(PCollection<FeatureRowExtended> input) {
//       this.inputs.add(input.apply(getName(), ParDo.of(new Identity(getName()))));
//       return PDone.in(input.getPipeline());
//     }
//   }
// }
