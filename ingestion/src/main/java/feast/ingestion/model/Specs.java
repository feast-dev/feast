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
// package feast.ingestion.model;
//
// import static com.google.common.base.Predicates.not;
//
// import com.google.common.base.Preconditions;
// import com.google.common.base.Strings;
// import feast.specs.EntitySpecProto.EntitySpec;
// import feast.specs.FeatureSpecProto.FeatureSpec;
// import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
// import feast.specs.ImportJobSpecsProto.SourceSpec;
// import feast.specs.ImportSpecProto.Field;
// import feast.specs.ImportSpecProto.ImportSpec;
// import feast.specs.StorageSpecProto.StorageSpec;
// import java.io.Serializable;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;
// import lombok.Getter;
// import lombok.ToString;
// import lombok.extern.slf4j.Slf4j;
//
// @Slf4j
// @ToString
// @Getter
// public class Specs implements Serializable {
//
//   SourceSpec sourceSpec;
//   Map<String, EntitySpec> entitySpecs = new HashMap<>();
//   Map<String, FeatureSpec> featureSpecs;
//   StorageSpec sinkStorageSpec;
//   StorageSpec errorsStoreSpec;
//
//   @Getter
//   private String jobName;
//
//   public Specs(String jobName, ImportJobSpecs importJobSpecs) {
//     this.jobName = jobName;
//     this.sourceSpec = importJobSpecs.getSourceSpec();
//     if (importJobSpecs != null) {
//
//       this.entitySpecs.put(importJobSpecs.getEntitySpec().getName(), importJobSpecs.getEntitySpec());
//       this.featureSpecs = importJobSpecs.getFeatureSpecsList().stream().collect(Collectors.toMap(
//           FeatureSpec::getId,
//           featureSpec -> featureSpec
//       ));
//
//       this.sinkStorageSpec = importJobSpecs.getSinkStorageSpec();
//       this.errorsStoreSpec = importJobSpecs.getErrorsStorageSpec();
//     }
//   }
//
//   public static Specs of(String jobName, ImportJobSpecs importJobSpecs) {
//     return new Specs(jobName, importJobSpecs);
//   }
//
//   public EntitySpec getEntitySpec(String entityName) {
//     Preconditions.checkArgument(
//         entitySpecs.containsKey(entityName),
//         String.format("Unknown entity %s, spec was not initialized", entityName));
//     return entitySpecs.get(entityName);
//   }
//
//   public FeatureSpec getFeatureSpec(String featureId) {
//     Preconditions.checkArgument(
//         featureSpecs.containsKey(featureId),
//         String.format("Unknown feature %s, spec was not initialized", featureId));
//     return featureSpecs.get(featureId);
//   }
// }
