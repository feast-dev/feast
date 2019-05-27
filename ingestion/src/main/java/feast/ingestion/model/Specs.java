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

package feast.ingestion.model;

import static com.google.common.base.Predicates.not;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
public class Specs implements Serializable {

  @Getter
  ImportSpec importSpec;
  @Getter
  Map<String, EntitySpec> entitySpecs;
  @Getter
  Map<String, FeatureSpec> featureSpecs;
  @Getter
  StorageSpec servingStorageSpec;
  @Getter
  StorageSpec warehouseStorageSpec;
  @Getter
  StorageSpec errorsStoreSpec;

  @Getter
  private String jobName;

  public Specs(String jobName, ImportJobSpecs importJobSpecs) {
    this.jobName = jobName;
    if (importJobSpecs != null) {
      this.importSpec = importJobSpecs.getImportSpec();

      this.entitySpecs = importJobSpecs.getEntitySpecsList().stream().collect(Collectors.toMap(
          EntitySpec::getName,
          entitySpec -> entitySpec
      ));
      this.featureSpecs = importJobSpecs.getFeatureSpecsList().stream().collect(Collectors.toMap(
          FeatureSpec::getId,
          featureSpec -> featureSpec
      ));

      this.servingStorageSpec = importJobSpecs.getServingStorageSpec();
      this.warehouseStorageSpec = importJobSpecs.getWarehouseStorageSpec();
      this.errorsStoreSpec = importJobSpecs.getErrorsStorageSpec();
    }
  }

  public static Specs of(String jobName, ImportJobSpecs importJobSpecs) {
    return new Specs(jobName, filterToUtilized(importJobSpecs));
  }

  static ImportJobSpecs filterToUtilized(ImportJobSpecs importJobSpecs) {
    ImportSpec importSpec = importJobSpecs.getImportSpec();
    List<Field> fields = importSpec.getSchema().getFieldsList();
    List<String> featureIds = fields.stream().map(Field::getFeatureId)
        .filter(not(Strings::isNullOrEmpty))
        .collect(Collectors.toList());
    List<String> entityNames = importSpec.getEntitiesList();

    List<FeatureSpec> featureSpecs =
        importJobSpecs.getFeatureSpecsList().stream()
            .filter(featureSpec -> featureIds.contains(featureSpec.getId()))
            .collect(Collectors.toList());
    List<EntitySpec> entitySpecs =
        importJobSpecs.getEntitySpecsList().stream()
            .filter(entitySpec -> entityNames.contains(entitySpec.getName()))
            .collect(Collectors.toList());
    return importJobSpecs.toBuilder()
        .clearFeatureSpecs().addAllFeatureSpecs(featureSpecs)
        .clearEntitySpecs().addAllEntitySpecs(entitySpecs).build();
  }

  public void validate() {
    for (String entityName : importSpec.getEntitiesList()) {
      getEntitySpec(entityName);
    }
    for (Field field : importSpec.getSchema().getFieldsList()) {
      if (!Strings.isNullOrEmpty(field.getFeatureId())) {
        getFeatureSpec(field.getFeatureId());
      }
    }
  }

  public EntitySpec getEntitySpec(String entityName) {
    Preconditions.checkArgument(
        entitySpecs.containsKey(entityName),
        String.format("Unknown entity %s, spec was not initialized", entityName));
    return entitySpecs.get(entityName);
  }

  public FeatureSpec getFeatureSpec(String featureId) {
    Preconditions.checkArgument(
        featureSpecs.containsKey(featureId),
        String.format("Unknown feature %s, spec was not initialized", featureId));
    return featureSpecs.get(featureId);
  }
}
