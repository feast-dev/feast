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

import static feast.core.util.TypeConversion.convertJsonStringToMap;
import static feast.core.util.TypeConversion.convertTagStringToList;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.storage.BigQueryStorageManager;
import feast.core.util.TypeConversion;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

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

  @Id
  private String id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "owner", nullable = false)
  private String owner;

  @Column(name = "description", nullable = false)
  private String description;

  @Column(name = "uri", nullable = false)
  private String uri;

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

  @Column(name = "warehouse_view")
  private String warehouseView;

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
      FeatureGroupInfo featureGroupInfo) {
    this.id = spec.getId();
    this.name = spec.getName();
    this.owner = spec.getOwner();
    this.description = spec.getDescription();
    this.uri = spec.getUri();
    this.valueType = spec.getValueType();
    this.entity = entityInfo;
    this.featureGroup = featureGroupInfo;
    this.warehouseView = "";
    this.tags = String.join(",", spec.getTagsList());
    this.options = TypeConversion.convertMapToJsonString(spec.getOptionsMap());
  }

  public FeatureInfo(FeatureInfo other) {
    this.id = other.id;
    this.name = other.name;
    this.owner = other.owner;
    this.description = other.description;
    this.uri = other.uri;
    this.valueType = other.valueType;
    this.entity = other.entity;
    this.featureGroup = other.featureGroup;
    this.tags = other.tags;
    this.options = other.options;
    this.warehouseView = other.warehouseView;
    this.enabled = other.enabled;
    this.setLastUpdated(other.getLastUpdated());
    this.setCreated(other.getCreated());
  }

  /**
   * Get the feature spec associated with this record. The spec returned by this method will not
   * resolve inheritance from associated feature groups.
   */
  public FeatureSpec getFeatureSpec() {
    FeatureSpec.Builder builder =
        FeatureSpec.newBuilder()
            .setId(id)
            .setName(name)
            .setOwner(owner)
            .setDescription(description)
            .setUri(uri)
            .setValueType(valueType)
            .setEntity(entity.getName())
            .addAllTags(convertTagStringToList(tags))
            .putAllOptions(convertJsonStringToMap(options));
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
    List<String> resolvedTags = new ArrayList<>();
    resolvedTags.addAll(convertTagStringToList(featureInfoCopy.tags));
    resolvedTags.addAll(convertTagStringToList(featureGroup.getTags()));
    featureGroup.getOptions();
    featureInfoCopy.tags = String.join(",", resolvedTags);

    return featureInfoCopy;
  }

  /**
   * Get the feature detail containing both spec and metadata, associated with this record.
   */
  public FeatureDetail getFeatureDetail(StorageSpecs storageSpecs) {
    return FeatureDetail.newBuilder()
        .setSpec(this.getFeatureSpec())
        .setBigqueryView(!Strings.isNullOrEmpty(warehouseView) ? warehouseView
            : createWarehouseLink(storageSpecs.getWarehouseStorageSpec()))
        .setEnabled(this.enabled)
        .setLastUpdated(TypeConversion.convertTimestamp(this.getLastUpdated()))
        .setCreated(TypeConversion.convertTimestamp(this.getCreated()))
        .build();
  }

  protected String createWarehouseLink(StorageSpec storageSpec) {
    if (storageSpec == null || !storageSpec.getType().equals(BigQueryStorageManager.TYPE)) {
      return "N.A.";
    }

    switch (storageSpec.getType()){
      case BigQueryStorageManager.TYPE:
        String projectId = storageSpec
          .getOptionsOrDefault(BigQueryStorageManager.OPT_BIGQUERY_PROJECT, null);
        String dataset = storageSpec
          .getOptionsOrDefault(BigQueryStorageManager.OPT_BIGQUERY_DATASET, null);

        return String.format(
          "https://bigquery.cloud.google.com/table/%s:%s.%s_view",
          projectId, dataset, entity.getName());
      default:
        return "N.A";
    }
  }

  /**
   * Updates the feature info with specifications from the incoming feature spec.
   *
   * <p>TODO: maybe allow changes to id, store etc if no jobs are feeding into this feature
   *
   * @param update new feature spec
   */
  public void update(FeatureSpec update) throws IllegalArgumentException {
    if (!isLegalUpdate(update)) {
      throw new IllegalArgumentException(
          "Feature already exists. Update only allowed for fields: [owner, description, uri, tags]");
    }
    this.owner = update.getOwner();
    this.description = update.getDescription();
    this.uri = update.getUri();
    this.tags = String.join(",", update.getTagsList());
  }

  private boolean isLegalUpdate(FeatureSpec update) {
    FeatureSpec spec = this.getFeatureSpec();
    return spec.getName().equals(update.getName())
        && spec.getEntity().equals(update.getEntity())
        && spec.getValueType().equals(update.getValueType())
        && spec.getGroup().equals(update.getGroup())
        && Maps.difference(spec.getOptionsMap(), update.getOptionsMap()).areEqual();
  }
}
