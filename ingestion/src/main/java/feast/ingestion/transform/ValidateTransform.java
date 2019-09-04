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
// import org.apache.beam.sdk.transforms.PTransform;
// import feast.ingestion.model.Specs;
// import feast.ingestion.transform.fn.ValidateFeatureRowsDoFn;
// import feast.ingestion.values.PFeatureRows;
// import feast.specs.ImportSpecProto.Field;
// import feast.specs.ImportSpecProto.ImportSpec;
//
// public class ValidateTransform extends PTransform<PFeatureRows, PFeatureRows> {
//
//   private Specs specs;
//
//   public ValidateTransform(Specs specs) {
//     this.specs = specs;
//   }
//
//   @Override
//   public PFeatureRows expand(PFeatureRows input) {
//     //   }
//     // }
//
//     return input.applyDoFn("Validate feature rows dofn", new ValidateFeatureRowsDoFn(specs));
//   }
// }
