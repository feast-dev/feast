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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@AllArgsConstructor
public class Specs implements Serializable {

  @Getter
  private String jobName;
  private ImportJobSpecs importJobSpecs;

  public static Specs of(String jobName, ImportJobSpecs importJobSpecs) {
    return new Specs(jobName, filterToUtilized(importJobSpecs));
  }

  static ImportJobSpecs filterToUtilized(ImportJobSpecs importJobSpecs) {
    Specs.validate(importJobSpecs);
    ImportSpec importSpec = importJobSpecs.getImport();
    List<Field> fields = importSpec.getSchema().getFieldsList();
    List<String> featureIds = new ArrayList<>();
    List<String> entityNames = importSpec.getEntitiesList();
    for (Field field : fields) {
      if (!field.getFeatureId().isEmpty()) {
        featureIds.add(field.getFeatureId());
      }
    }

    Map<String, FeatureSpec> featureSpecs = Maps
        .filterEntries(importJobSpecs.getFeaturesMap(), (entry) ->
            featureIds.contains(entry.getValue().getId()));
    Map<String, EntitySpec> entitySpecs = Maps
        .filterEntries(importJobSpecs.getEntitiesMap(), (entry) ->
            entityNames.contains(entry.getValue().getName()));

    return importJobSpecs.toBuilder()
        .clearFeatures().putAllFeatures(featureSpecs)
        .clearEntities().putAllEntities(entitySpecs).build();
  }

  public static void validate(ImportJobSpecs importJobSpecs) {
    // Sanity checks that our maps are built correctly
    for (Entry<String, FeatureSpec> entry : importJobSpecs.getFeaturesMap().entrySet()) {
      Preconditions.checkArgument(entry.getKey().equals(entry.getValue().getId()),
          String.format("Feature id does not match spec %s!=%s", entry.getKey(),
              entry.getValue().getId()));
    }
    for (Entry<String, EntitySpec> entry : importJobSpecs.getEntitiesMap().entrySet()) {
      Preconditions.checkArgument(entry.getKey().equals(entry.getValue().getName()),
          String.format("Entity name does not match spec %s!=%s", entry.getKey(),
              entry.getValue().getName()));
    }

    for (FeatureSpec featureSpec : importJobSpecs.getFeaturesMap().values()) {
      // Check that feature has a matching entity
      Preconditions.checkArgument(
          importJobSpecs.containsEntities(featureSpec.getEntity()),
          String.format(
              "Feature %s references unknown entity %s",
              featureSpec.getId(), featureSpec.getEntity()));
    }
  }

  public ImportSpec getImportSpec() {
    return importJobSpecs.getImport();
  }

  public StorageSpec getServingStorageSpec() {
    return importJobSpecs.getServingStorage();
  }

  public StorageSpec getWarehouseStorageSpec() {
    return importJobSpecs.getWarehouseStorage();
  }

  public Map<String, EntitySpec> getEntitySpecs() {
    return importJobSpecs.getEntitiesMap();
  }

  public Map<String, FeatureSpec> getFeatureSpecs() {
    return importJobSpecs.getFeaturesMap();
  }

  public void validate() {
    validate(importJobSpecs);
  }

  public EntitySpec getEntitySpec(String entityName) {
    Preconditions.checkArgument(
        importJobSpecs.containsEntities(entityName),
        String.format("Unknown entity %s, spec was not initialized", entityName));
    return importJobSpecs.getEntitiesOrThrow(entityName);
  }

  public FeatureSpec getFeatureSpec(String featureId) {
    Preconditions.checkArgument(
        importJobSpecs.containsFeatures(featureId),
        String.format("Unknown feature %s, spec was not initialized", featureId));
    return importJobSpecs.getFeaturesOrThrow(featureId);
  }
}
