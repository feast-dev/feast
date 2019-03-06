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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
@ToString
public class Specs implements Serializable {

  @Getter
  private String jobName;
  private ImportJobSpecs importJobSpecs;

  public static Specs of(String jobName, ImportJobSpecs importJobSpecs) {
    Specs.SpecsBuilder specsBuilder = Specs.builder().jobName(jobName)
        .importJobSpecs(filterToUtilized(importJobSpecs));
    return specsBuilder.build();
  }

  public static ImportJobSpecs filterToUtilized(ImportJobSpecs importJobSpecs) {
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
    Map<String, StorageSpec> storageSpecs = new HashMap<>();

    for (FeatureSpec featureSpec : featureSpecs.values()) {
      String servingStoreId = featureSpec.getDataStores().getServing().getId();
      String warehouseStoreId = featureSpec.getDataStores().getWarehouse().getId();
      if (!servingStoreId.isEmpty()) {
        storageSpecs.put(servingStoreId, importJobSpecs.getStorageOrThrow(servingStoreId));
      }
      if (!warehouseStoreId.isEmpty()) {
        storageSpecs.put(warehouseStoreId, importJobSpecs.getStorageOrThrow(warehouseStoreId));
      }
    }
    return importJobSpecs.toBuilder()
        .clearFeatures().putAllFeatures(featureSpecs)
        .clearEntities().putAllEntities(entitySpecs)
        .clearStorage().putAllStorage(storageSpecs).build();
  }

  public ImportSpec getImportSpec() {
    return importJobSpecs.getImport();
  }

  public Map<String, StorageSpec> getStorageSpecs() {
    return importJobSpecs.getStorageMap();
  }

  public Map<String, EntitySpec> getEntitySpecs() {
    return importJobSpecs.getEntitiesMap();
  }

  public Map<String, FeatureSpec> getFeatureSpecs() {
    return importJobSpecs.getFeaturesMap();
  }

  public void validate() {
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
    for (Entry<String, StorageSpec> entry : importJobSpecs.getStorageMap().entrySet()) {
      Preconditions.checkArgument(entry.getKey().equals(entry.getValue().getId()),
          String.format("Storage id does not match spec %s!=%s", entry.getKey(),
              entry.getValue().getId()));
    }

    for (FeatureSpec featureSpec : importJobSpecs.getFeaturesMap().values()) {
      // Check that feature has a matching entity
      Preconditions.checkArgument(
          importJobSpecs.containsEntities(featureSpec.getEntity()),
          String.format(
              "Feature %s references unknown entity %s",
              featureSpec.getId(), featureSpec.getEntity()));
      // Check that feature has a matching serving store
      if (!featureSpec.getDataStores().getServing().getId().isEmpty()) {
        Preconditions.checkArgument(
            importJobSpecs.containsStorage(featureSpec.getDataStores().getServing().getId()),
            String.format(
                "Feature %s references unknown serving store %s",
                featureSpec.getId(), featureSpec.getDataStores().getServing().getId()));
      }
      // Check that feature has a matching warehouse store
      if (!featureSpec.getDataStores().getWarehouse().getId().isEmpty()) {
        Preconditions.checkArgument(
            importJobSpecs.containsStorage(featureSpec.getDataStores().getWarehouse().getId()),
            String.format(
                "Feature %s references unknown warehouse store %s",
                featureSpec.getId(), featureSpec.getDataStores().getWarehouse().getId()));
      }
    }
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

  public List<FeatureSpec> getFeatureSpecByServingStoreId(String storeId) {
    List<FeatureSpec> out = new ArrayList<>();
    for (FeatureSpec featureSpec : importJobSpecs.getFeaturesMap().values()) {
      if (featureSpec.getDataStores().getServing().getId().equals(storeId)) {
        out.add(featureSpec);
      }
    }
    return out;
  }

  public StorageSpec getStorageSpec(String storeId) {
    Preconditions.checkArgument(
        importJobSpecs.containsStorage(storeId),
        String.format("Unknown store %s, spec was not initialized", storeId));
    return importJobSpecs.getStorageOrThrow(storeId);
  }
}
