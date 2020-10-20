/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.core.model;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.dao.EntityRepository;
import feast.core.util.TypeConversion;
import feast.proto.core.DataSourceProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureTableProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
@Entity
@Setter(AccessLevel.PRIVATE)
@Table(
    name = "feature_tables",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project_name"}))
public class FeatureTable extends AbstractTimestampEntity {

  @Id @GeneratedValue private long id;

  // Name of Feature Table
  @Column(name = "name", nullable = false)
  private String name;

  // Name of the Project that this FeatureTable belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "project_name")
  private Project project;

  // Features defined in this Feature Table
  @OneToMany(
      mappedBy = "featureTable",
      cascade = CascadeType.ALL,
      fetch = FetchType.EAGER,
      orphanRemoval = true)
  private Set<FeatureV2> features;

  // Entites to associate the features defined in this FeatureTable with
  @ManyToMany
  @JoinTable(
      name = "feature_tables_entities_v2",
      joinColumns = @JoinColumn(name = "feature_table_id"),
      inverseJoinColumns = @JoinColumn(name = "entity_v2_id"))
  private Set<EntityV2> entities;

  // User defined metadata labels serialized as JSON string.
  @Column(name = "labels", columnDefinition = "text")
  private String labelsJSON;

  // Max Age of the Features defined in this Feature Table in seconds
  @Column(name = "max_age", nullable = false)
  private long maxAgeSecs;

  // Streaming DataSource used to obtain data for features from a stream
  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "stream_source_id", nullable = true)
  private DataSource streamSource;

  // Batched DataSource used to obtain data for features from a batch of data
  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "batch_source_id", nullable = false)
  private DataSource batchSource;

  // Autoincrementing version no. of this FeatureTable.
  // Autoincrements every update made to the FeatureTable.
  @Column(name = "revision", nullable = false)
  private int revision;

  public FeatureTable() {};

  /**
   * Construct FeatureTable from Protobuf spec representation in the given project with entities
   * registered in entity repository.
   *
   * @param projectName the name of the project that the constructed FeatureTable belongs.
   * @param spec the Protobuf spec to construct the Feature from.
   * @param entityRepo {@link EntityRepository} used to resolve entity names.
   * @throws IllegalArgumentException if the Protobuf spec provided is invalid.
   * @return constructed FeatureTable from the given Protobuf spec.
   */
  public static FeatureTable fromProto(
      String projectName, FeatureTableSpec spec, EntityRepository entityRepo) {
    FeatureTable table = new FeatureTable();
    table.setName(spec.getName());
    table.setProject(new Project(projectName));

    Set<FeatureV2> features =
        spec.getFeaturesList().stream()
            .map(featureSpec -> FeatureV2.fromProto(table, featureSpec))
            .collect(Collectors.toSet());
    table.setFeatures(features);

    Set<EntityV2> entities =
        FeatureTable.resolveEntities(
            projectName, spec.getName(), entityRepo, spec.getEntitiesList());
    table.setEntities(entities);

    String labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
    table.setLabelsJSON(labelsJSON);

    table.setMaxAgeSecs(spec.getMaxAge().getSeconds());
    table.setBatchSource(DataSource.fromProto(spec.getBatchSource()));

    // Configure stream source only if set
    if (!spec.getStreamSource().equals(DataSourceProto.DataSource.getDefaultInstance())) {
      table.setStreamSource(DataSource.fromProto(spec.getStreamSource()));
    }

    return table;
  }

  /**
   * Update the FeatureTable from the given Protobuf representation.
   *
   * @param spec the Protobuf spec to update the FeatureTable from.
   * @throws IllegalArgumentException if the update will make prohibited changes.
   */
  public void updateFromProto(FeatureTableSpec spec) {
    // Check for prohibited changes made in spec:
    // - Name cannot be changed
    if (!getName().equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Updating the name of a registered FeatureTable is not allowed: %s to %s",
              getName(), spec.getName()));
    }
    // - Entities cannot be changed
    List<String> entityNames =
        getEntities().stream().map(EntityV2::getName).collect(Collectors.toList());
    if (!new HashSet<>(entityNames).equals(new HashSet<>(spec.getEntitiesList()))) {
      Collections.sort(entityNames);
      throw new IllegalArgumentException(
          String.format(
              "Updating the entities of a registered FeatureTable is not allowed: %s to %s",
              entityNames, spec.getEntitiesList()));
    }

    Map<String, FeatureSpecV2> updatedFeatures =
        spec.getFeaturesList().stream().collect(Collectors.toMap(FeatureSpecV2::getName, ft -> ft));

    // Update FeatureTable based on spec
    // Tombstone removed features, update existing features and create new features
    for (FeatureV2 existingFeature : features) {
      String existingFeatureName = existingFeature.getName();
      FeatureSpecV2 updatedFeatureSpec = updatedFeatures.get(existingFeatureName);
      if (updatedFeatureSpec == null) {
        existingFeature.archive();
      } else {
        existingFeature.updateFromProto(updatedFeatureSpec);
        updatedFeatures.remove(existingFeatureName);
      }
    }

    for (FeatureSpecV2 featureSpec : updatedFeatures.values()) {
      FeatureV2 newFeature = FeatureV2.fromProto(this, featureSpec);
      addFeature(newFeature);
    }

    // Update max age, source and labels.
    this.maxAgeSecs = spec.getMaxAge().getSeconds();
    this.labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());

    this.batchSource = DataSource.fromProto(spec.getBatchSource());
    if (!spec.getStreamSource().equals(DataSourceProto.DataSource.getDefaultInstance())) {
      this.streamSource = DataSource.fromProto(spec.getStreamSource());
    } else {
      this.streamSource = null;
    }

    // Bump revision no.
    this.revision++;
  }

  public void addFeature(FeatureV2 feature) {
    feature.setFeatureTable(this);
    features.add(feature);
  }

  /** Convert this Feature Table to its Protobuf representation */
  public FeatureTableProto.FeatureTable toProto() {
    // Convert field types to Protobuf compatible types
    Timestamp creationTime = TypeConversion.convertTimestamp(getCreated());
    Timestamp updatedTime = TypeConversion.convertTimestamp(getLastUpdated());

    List<FeatureSpecV2> featureSpecs =
        getFeatures().stream()
            .filter(feature -> !feature.isArchived())
            .map(FeatureV2::toProto)
            .collect(Collectors.toList());
    List<String> entityNames =
        getEntities().stream().map(EntityV2::getName).collect(Collectors.toList());
    Map<String, String> labels = TypeConversion.convertJsonStringToMap(getLabelsJSON());

    FeatureTableSpec.Builder spec =
        FeatureTableSpec.newBuilder()
            .setName(getName())
            .setMaxAge(Duration.newBuilder().setSeconds(getMaxAgeSecs()).build())
            .setBatchSource(getBatchSource().toProto())
            .addAllEntities(entityNames)
            .addAllFeatures(featureSpecs)
            .putAllLabels(labels);
    if (getStreamSource() != null) {
      spec.setStreamSource(getStreamSource().toProto());
    }

    return FeatureTableProto.FeatureTable.newBuilder()
        .setMeta(
            FeatureTableProto.FeatureTableMeta.newBuilder()
                .setRevision(getRevision())
                .setCreatedTimestamp(creationTime)
                .setLastUpdatedTimestamp(updatedTime)
                .build())
        .setSpec(spec.build())
        .build();
  }

  /** Use given entity repository to resolve entity names to entity native objects */
  private static Set<EntityV2> resolveEntities(
      String projectName, String tableName, EntityRepository repo, Collection<String> names) {
    return names.stream()
        .map(
            entityName -> {
              EntityV2 entity = repo.findEntityByNameAndProject_Name(entityName, projectName);
              if (entity == null) {
                throw new IllegalArgumentException(
                    String.format(
                        "Feature Table refers to no existent Entity: (table: %s, entity: %s, project: %s)",
                        tableName, entityName, projectName));
              }
              return entity;
            })
        .collect(Collectors.toSet());
  }

  /**
   * Determine whether a FeatureTable has all the specified labels.
   *
   * @param labelsFilter labels contain key-value mapping for labels attached to the FeatureTable
   * @return boolean True if Entity contains all labels in the labelsFilter
   */
  public boolean hasAllLabels(Map<String, String> labelsFilter) {
    Map<String, String> LabelsMap = this.getLabelsMap();
    for (String key : labelsFilter.keySet()) {
      if (!LabelsMap.containsKey(key) || !LabelsMap.get(key).equals(labelsFilter.get(key))) {
        return false;
      }
    }
    return true;
  }

  public Map<String, String> getLabelsMap() {
    return TypeConversion.convertJsonStringToMap(getLabelsJSON());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getName(),
        getProject(),
        getFeatures(),
        getEntities(),
        getMaxAgeSecs(),
        getBatchSource(),
        getStreamSource());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FeatureTable)) {
      return false;
    }

    FeatureTable other = (FeatureTable) o;

    return getName().equals(other.getName())
        && getProject().equals(other.getProject())
        && getLabelsJSON().equals(other.getLabelsJSON())
        && getFeatures().containsAll(other.getFeatures())
        && getEntities().containsAll(other.getEntities())
        && getMaxAgeSecs() == getMaxAgeSecs()
        && Optional.ofNullable(getBatchSource()).equals(Optional.ofNullable(other.getBatchSource()))
        && Optional.ofNullable(getStreamSource())
            .equals(Optional.ofNullable(other.getStreamSource()));
  }
}
