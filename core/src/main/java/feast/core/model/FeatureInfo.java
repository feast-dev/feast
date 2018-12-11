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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.storage.BigQueryStorageManager;
import feast.core.util.TypeConversion;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.ValueProto.ValueType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.util.Strings;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static feast.core.util.TypeConversion.convertJsonStringToMap;
import static feast.core.util.TypeConversion.convertTagStringToList;

/**
 * A row in the registry storing information about a single feature, including its relevant
 * metadata.
 */
@AllArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "features")
public class FeatureInfo extends AbstractTimestampEntity {

  @Id private String id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "owner", nullable = false)
  private String owner;

  @Column(name = "description", nullable = false)
  private String description;

  @Column(name = "uri", nullable = false)
  private String uri;

  @Enumerated(EnumType.STRING)
  private Granularity.Enum granularity;

  @Enumerated(EnumType.STRING)
  private ValueType.Enum valueType;

  @ManyToOne
  @JoinColumn(name = "entity")
  private EntityInfo entity;

  @ManyToOne(optional = true, fetch = FetchType.LAZY)
  @JoinColumn(name = "feature_group")
  private FeatureGroupInfo featureGroup;

  @Column(name = "tags")
  private String tags;

  @Column(name = "options")
  private String options;

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

  @Column(name = "big_query_view")
  private String bigQueryView;

  @ManyToMany(mappedBy = "features")
  private List<JobInfo> jobs;

  @Column(name = "enabled")
  private boolean enabled = true;

  public FeatureInfo() {
    super();
  }

  public FeatureInfo(
      FeatureSpec spec,
      EntityInfo entityInfo,
      StorageInfo servingStore,
      StorageInfo warehouseStore,
      FeatureGroupInfo featureGroupInfo) {
    this.id = spec.getId();
    this.name = spec.getName();
    this.owner = spec.getOwner();
    this.description = spec.getDescription();
    this.uri = spec.getUri();
    this.granularity = spec.getGranularity();
    this.valueType = spec.getValueType();
    this.entity = entityInfo;
    this.featureGroup = featureGroupInfo;
    this.tags = String.join(",", spec.getTagsList());
    this.options = TypeConversion.convertMapToJsonString(spec.getOptionsMap());
    if (spec.getDataStores() != null) {
      this.servingStore = servingStore;
      this.servingStoreOpts =
              TypeConversion.convertMapToJsonString(spec.getDataStores().getServing().getOptionsMap());
      this.warehouseStore = warehouseStore;
      this.warehouseStoreOpts =
              TypeConversion.convertMapToJsonString(spec.getDataStores().getWarehouse().getOptionsMap());
    }
    this.bigQueryView = createBigqueryViewLink(warehouseStore);
  }

  public FeatureInfo(FeatureInfo other) {
    this.id = other.id;
    this.name = other.name;
    this.owner = other.owner;
    this.description = other.description;
    this.uri = other.uri;
    this.granularity = other.granularity;
    this.valueType = other.valueType;
    this.entity = other.entity;
    this.featureGroup = other.featureGroup;
    this.tags = other.tags;
    this.options = other.options;
    this.warehouseStore = other.warehouseStore;
    this.warehouseStoreOpts = other.warehouseStoreOpts;
    this.servingStore = other.servingStore;
    this.servingStoreOpts = other.servingStoreOpts;
    this.bigQueryView = other.bigQueryView;
    this.enabled = other.enabled;
    this.setLastUpdated(other.getLastUpdated());
    this.setCreated(other.getCreated());
  }

  /**
   * Get the feature spec associated with this record. The spec returned by this method will not
   * resolve inheritance from associated feature groups.
   */
  public FeatureSpec getFeatureSpec() {
    DataStores.Builder dataStoreBuilder = DataStores.newBuilder();
    if (servingStore != null) {
      DataStore servingDataStore = buildDataStore(servingStore.getId(), servingStoreOpts);
      dataStoreBuilder.setServing(servingDataStore);
    }
    if (warehouseStore != null) {
      DataStore warehouseDataStore = buildDataStore(warehouseStore.getId(), warehouseStoreOpts);
      dataStoreBuilder.setWarehouse(warehouseDataStore);
    }
    DataStores dataStores = dataStoreBuilder.build();

    FeatureSpec.Builder builder =
        FeatureSpec.newBuilder()
            .setId(id)
            .setName(name)
            .setOwner(owner)
            .setDescription(description)
            .setUri(uri)
            .setGranularity(granularity)
            .setValueType(valueType)
            .setEntity(entity.getName())
            .addAllTags(convertTagStringToList(tags))
            .putAllOptions(convertJsonStringToMap(options))
            .setDataStores(dataStores);
    if (featureGroup != null) {
      builder.setGroup(featureGroup.getId());
    }
    return builder.build();
  }

  /*
   * Resolve the feature spec with its group settings.
   */
  public FeatureInfo resolve() {
    if (featureGroup == null) {
      return this;
    }
    FeatureInfo featureInfoCopy = new FeatureInfo(this);
    if (featureInfoCopy.servingStore == null) {
      featureInfoCopy.servingStore = featureGroup.getServingStore();
    }
    if (featureInfoCopy.warehouseStore == null) {
      featureInfoCopy.warehouseStore = featureGroup.getWarehouseStore();
    }

    List<String> resolvedTags = new ArrayList<>();
    resolvedTags.addAll(convertTagStringToList(featureInfoCopy.tags));
    resolvedTags.addAll(convertTagStringToList(featureGroup.getTags()));
    featureInfoCopy.tags = String.join(",", resolvedTags);

    featureInfoCopy.bigQueryView = createBigqueryViewLink(featureInfoCopy.warehouseStore);
    return featureInfoCopy;
  }

  /** Get the feature detail containing both spec and metadata, associated with this record. */
  public FeatureDetail getFeatureDetail() {
    return FeatureDetail.newBuilder()
        .setSpec(this.getFeatureSpec())
        .setBigqueryView(this.bigQueryView)
        .setEnabled(this.enabled)
        .setLastUpdated(TypeConversion.convertTimestamp(this.getLastUpdated()))
        .setCreated(TypeConversion.convertTimestamp(this.getCreated()))
        .build();
  }

  private DataStore buildDataStore(String id, String opts) {
    DataStore.Builder builder = DataStore.newBuilder();
    if (id != null) {
      builder.setId(id);
    }
    if (opts != null) {
      builder.putAllOptions(convertJsonStringToMap(opts));
    }
    return builder.build();
  }

  private String createBigqueryViewLink(StorageInfo warehouseStore) {
    if (warehouseStore == null || !warehouseStore.getType().equals(BigQueryStorageManager.TYPE)) {
      return "N.A.";
    }
    Map<String, String> opts = convertJsonStringToMap(warehouseStore.getOptions());
    String projectId = opts.get(BigQueryStorageManager.OPT_BIGQUERY_PROJECT);
    String dataset = opts.get(BigQueryStorageManager.OPT_BIGQUERY_DATASET);

    return String.format(
        "https://bigquery.cloud.google.com/table/%s:%s.%s_%s_view",
        projectId, dataset, entity.getName(), granularity.toString().toLowerCase());
  }

  /**
   * Checks if this is eq to the other given feature
   *
   * @param otherFeature
   * @return boolean
   */
  public boolean eq(FeatureInfo otherFeature) {
    return otherFeature.getId() == this.id &&
            otherFeature.getEntity().getName() == this.entity.getName() &&
            otherFeature.getOwner() == this.owner &&
            otherFeature.getUri() == this.uri &&
            otherFeature.getDescription() == this.description &&
            otherFeature.getGranularity() == this.granularity &&
            otherFeature.getValueType() == this.valueType &&
            otherFeature.getTags() == this.tags &&
            otherFeature.getOptions() == this.options &&
            getFeatureGroupId(otherFeature.getFeatureGroup()) == getFeatureGroupId(this.getFeatureGroup()) &&
            otherFeature.getServingStoreOpts() == this.servingStoreOpts &&
            getStorageId(otherFeature.getServingStore()) == getStorageId(this.getServingStore()) &&
            otherFeature.getWarehouseStoreOpts() == this.warehouseStoreOpts &&
            getStorageId(otherFeature.getWarehouseStore()) == getStorageId(this.getWarehouseStore());
  }

  private String getFeatureGroupId(FeatureGroupInfo featureGroupInfo) {
    return featureGroupInfo == null ? "" : featureGroupInfo.getId();
  }
  private String getStorageId(StorageInfo storage) {
    return storage == null ? "" : storage.getId();
  }
}
