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
// package feast.ingestion.transform.fn;
//
// import static com.google.common.base.Preconditions.checkArgument;
// import static com.google.common.base.Preconditions.checkNotNull;
//
// import com.google.common.base.Preconditions;
// import com.google.common.base.Strings;
// import com.google.common.collect.Lists;
// import com.google.protobuf.util.Timestamps;
// import feast.ingestion.exceptions.ValidationException;
// import feast.ingestion.metrics.FeastMetrics;
// import feast.ingestion.model.Specs;
// import feast.ingestion.model.Values;
// import feast.specs.EntitySpecProto.EntitySpec;
// import feast.specs.FeatureSpecProto.FeatureSpec;
// import feast.specs.ImportSpecProto.Field;
// import feast.specs.ImportSpecProto.ImportSpec;
// import feast.store.serving.FeatureServingFactory;
// import feast.store.serving.FeatureServingFactoryService;
// import feast.store.warehouse.FeatureWarehouseFactory;
// import feast.store.warehouse.FeatureWarehouseFactoryService;
// import feast.types.FeatureProto.Feature;
// import feast.types.FeatureRowProto.FeatureRow;
// import feast.types.ValueProto.ValueType;
// import java.util.ArrayList;
// import java.util.HashSet;
// import java.util.List;
// import java.util.Set;
//
// public class ValidateFeatureRowsDoFn extends BaseFeatureDoFn {
//
//   private final List<String> featureIds = new ArrayList<>();
//
//   private Set<String> supportedServingTypes = new HashSet<>();
//   private Set<String> supportedWarehouseTypes = new HashSet<>();
//
//   private Specs specs;
//
//   public ValidateFeatureRowsDoFn(Specs specs) {
//     this.specs = specs;
//   }
//
//   @Setup
//   public void setup() {
//     specs.getFeatureSpecs().keySet().stream().forEach(featureIds::add);
//     for (FeatureServingFactory store : FeatureServingFactoryService.getAll()) {
//       supportedServingTypes.add(store.getType());
//     }
//     for (FeatureWarehouseFactory store : FeatureWarehouseFactoryService.getAll()) {
//       supportedWarehouseTypes.add(store.getType());
//     }
//   }
//
//   @Override
//   public void processElementImpl(ProcessContext context) {
//     FeatureRow row = context.element().getRow();
//     EntitySpec entitySpec = specs.getEntitySpec(row.getEntityName());
//     Preconditions.checkNotNull(entitySpec, "Entity spec not found for " + row.getEntityName());
//
//     try {
//       checkArgument(!row.getEntityKey().isEmpty(), "Entity key must not be empty");
//       checkArgument(!row.getEntityName().isEmpty(), "Entity name must not be empty");
//
//       checkArgument(
//           specs.getEntitySpecs().keySet().contains(row.getEntityName()),
//           String.format(
//               "Row entity not found in import spec entities. entity=%s", row.getEntityName()));
//
//       checkArgument(row.hasEventTimestamp(), "Must have eventTimestamp set");
//       Timestamps.checkValid(row.getEventTimestamp());
//
//       checkArgument(row.getFeaturesCount() > 0, "Must have at least one feature set");
//
//       for (Feature feature : row.getFeaturesList()) {
//         FeatureSpec featureSpec = specs.getFeatureSpec(feature.getId());
//         checkNotNull(
//             featureSpec, String.format("Feature spec not found featureId=%s", feature.getId()));
//
//         checkArgument(
//             featureSpec.getEntity().equals(row.getEntityName()),
//             String.format(
//                 "Feature must have same entity as row. featureId=%s FeatureRow.entityName=%s FeatureSpec.entity=%s",
//                 feature.getId(), row.getEntityName(), featureSpec.getEntity()));
//
//         ValueType.Enum expectedType = featureSpec.getValueType();
//         ValueType.Enum actualType = Values.toValueType(feature.getValue());
//         checkArgument(
//             expectedType.equals(actualType),
//             String.format("Invalid value type, expected %s, actual %s", expectedType, actualType));
//
//         if (featureIds.size() > 0) {
//           checkArgument(
//               featureIds.contains(feature.getId()),
//               String.format(
//                   "Unexpected feature that was not specified in import spec. featureId=%s",
//                   feature.getId()));
//         }
//       }
//       FeastMetrics.inc(context.element().getRow(), "valid");
//       context.output(context.element());
//     } catch (IllegalArgumentException e) {
//       throw new ValidationException(e.getMessage(), e);
//     }
//   }
// }
