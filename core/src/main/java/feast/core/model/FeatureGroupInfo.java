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

package feast.core.model;

import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.core.util.TypeConversion;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

/**
 * A row in the registry storing information about a single feature group, including its relevant
 * metadata.
 */
@AllArgsConstructor
@Entity
@Getter
@Setter
@Table(name = "feature_groups")
public class FeatureGroupInfo extends AbstractTimestampEntity {

  @Id private String id;

  @Column(name = "tags")
  private String tags;

  @ManyToOne
  @JoinColumn(name = "serving_store_id")
  private StorageInfo servingStore;

  @Column(name = "serving_store_opts")
  private String servingStoreOpts;

  @ManyToOne
  @JoinColumn(name = "warehouse_store_id")
  private StorageInfo warehouseStore;

  @Column(name = "warehouse_store_opts")
  private String warehouseStoreOpts;

  public FeatureGroupInfo() {
    super();
  }

  public FeatureGroupInfo(
      FeatureGroupSpec spec, StorageInfo servingStore, StorageInfo warehouseStore) {
    this.id = spec.getId();
    this.tags = String.join(",", spec.getTagsList());
    this.servingStore = servingStore;
    this.warehouseStore = warehouseStore;
    this.servingStoreOpts =
        TypeConversion.convertMapToJsonString(spec.getDataStores().getServing().getOptionsMap());
    this.warehouseStoreOpts =
        TypeConversion.convertMapToJsonString(spec.getDataStores().getWarehouse().getOptionsMap());
  }

  /** Get the feature group spec associated with this record. */
  public FeatureGroupSpec getFeatureGroupSpec() {
    DataStore servingDataStore =
        DataStore.newBuilder()
            .setId(servingStore.getId())
            .putAllOptions(TypeConversion.convertJsonStringToMap(servingStoreOpts))
            .build();
    DataStore warehouseDataStore =
        DataStore.newBuilder()
            .setId(warehouseStore.getId())
            .putAllOptions(TypeConversion.convertJsonStringToMap(warehouseStoreOpts))
            .build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setWarehouse(warehouseDataStore)
            .setServing(servingDataStore)
            .build();
    return FeatureGroupSpec.newBuilder()
        .setId(id)
        .addAllTags(TypeConversion.convertTagStringToList(tags))
        .setDataStores(dataStores)
        .build();
  }

  /**
   * Get the feature group detail containing both spec and metadata, associated with this record.
   */
  public FeatureGroupDetail getFeatureGroupDetail() {
    return FeatureGroupDetail.newBuilder()
        .setSpec(this.getFeatureGroupSpec())
        .setLastUpdated(TypeConversion.convertTimestamp(this.getLastUpdated()))
        .build();
  }

  public void update(FeatureGroupSpec update) throws IllegalArgumentException {
    if (!isLegalUpdate(update)) {
      throw new IllegalArgumentException(
          "Feature group already exists. Update only allowed for fields: [tags]");
    }
    this.tags = String.join(",", update.getTagsList());
  }

  private boolean isLegalUpdate(FeatureGroupSpec update) {
    DataStore updatedWarehouseStore =
        update.getDataStores().hasWarehouse() ? update.getDataStores().getWarehouse() : null;
    DataStore updatedServingStore =
        update.getDataStores().hasServing() ? update.getDataStores().getServing() : null;
    return isStoreEqual(this.warehouseStore, this.warehouseStoreOpts, updatedWarehouseStore)
        && isStoreEqual(this.servingStore, this.servingStoreOpts, updatedServingStore);
  }

  private boolean isStoreEqual(StorageInfo oldStore, String oldStoreOpts, DataStore newStore) {
    return getStorageId(oldStore).equals(newStore == null ? "" : newStore.getId())
        && oldStoreOpts.equals(
            newStore == null
                ? ""
                : TypeConversion.convertMapToJsonString(newStore.getOptionsMap()));
  }

  private String getStorageId(StorageInfo storage) {
    return storage == null ? "" : storage.getId();
  }
}
