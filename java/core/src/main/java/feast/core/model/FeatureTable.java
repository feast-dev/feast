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

import static feast.common.models.FeatureV2.getFeatureStringRef;

import com.google.common.hash.Hashing;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.dao.EntityRepository;
import feast.core.util.TypeConversion;
import feast.proto.core.DataSourceProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureTableProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.serving.ServingAPIProto;
import java.util.*;
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
  @ManyToMany(fetch = FetchType.EAGER)
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

  @Column(name = "is_deleted", nullable = false)
  private boolean isDeleted;

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
   * @param projectName project name
   * @param spec the Protobuf spec to update the FeatureTable from.
   * @param entityRepo repository
   * @throws IllegalArgumentException if the update will make prohibited changes.
   */
  public void updateFromProto(
      String projectName, FeatureTableSpec spec, EntityRepository entityRepo) {
    // Check for prohibited changes made in spec:
    // - Name cannot be changed
    if (!getName().equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Updating the name of a registered FeatureTable is not allowed: %s to %s",
              getName(), spec.getName()));
    }
    // Update Entities if changed
    Set<EntityV2> entities =
        FeatureTable.resolveEntities(
            projectName, spec.getName(), entityRepo, spec.getEntitiesList());
    this.setEntities(entities);

    // Update FeatureTable based on spec
    // Update existing features, create new feature, drop missing features
    Map<String, FeatureV2> existingFeatures =
        getFeatures().stream().collect(Collectors.toMap(FeatureV2::getName, feature -> feature));
    this.features.clear();
    this.features.addAll(
        spec.getFeaturesList().stream()
            .map(
                featureSpec -> {
                  if (!existingFeatures.containsKey(featureSpec.getName())) {
                    // Create new Feature based on spec
                    return FeatureV2.fromProto(this, featureSpec);
                  }
                  // Update existing feature based on spec
                  FeatureV2 feature = existingFeatures.get(featureSpec.getName());
                  feature.updateFromProto(featureSpec);
                  return feature;
                })
            .collect(Collectors.toSet()));

    this.maxAgeSecs = spec.getMaxAge().getSeconds();
    this.labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());

    this.batchSource = DataSource.fromProto(spec.getBatchSource());
    if (!spec.getStreamSource().equals(DataSourceProto.DataSource.getDefaultInstance())) {
      this.streamSource = DataSource.fromProto(spec.getStreamSource());
    } else {
      this.streamSource = null;
    }

    // Set isDeleted to false
    this.setDeleted(false);

    // Bump revision no.
    this.revision++;
  }

  /**
   * Convert this Feature Table to its Protobuf representation
   *
   * @return protobuf representation
   */
  public FeatureTableProto.FeatureTable toProto() {
    // Convert field types to Protobuf compatible types
    Timestamp creationTime = TypeConversion.convertTimestamp(getCreated());
    Timestamp updatedTime = TypeConversion.convertTimestamp(getLastUpdated());
    String metadataHashBytes = this.protoHash();

    List<FeatureSpecV2> featureSpecs =
        getFeatures().stream().map(FeatureV2::toProto).collect(Collectors.toList());
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
                .setHash(metadataHashBytes)
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
   * Return a boolean to indicate if FeatureTable contains all specified entities.
   *
   * @param entitiesFilter contain entities that should be attached to the FeatureTable
   * @return boolean True if FeatureTable contains all entities in the entitiesFilter
   */
  public boolean hasAllEntities(List<String> entitiesFilter) {
    Set<String> allEntitiesName =
        this.getEntities().stream().map(entity -> entity.getName()).collect(Collectors.toSet());
    return allEntitiesName.equals(new HashSet<>(entitiesFilter));
  }

  /**
   * Returns a map of Feature references and Features if FeatureTable's Feature contains all labels
   * in the labelsFilter
   *
   * @param labelsFilter contain labels that should be attached to FeatureTable's features
   * @return Map of Feature references and Features
   */
  public Map<String, FeatureV2> getFeaturesByLabels(Map<String, String> labelsFilter) {
    Map<String, FeatureV2> validFeaturesMap;
    List<FeatureV2> validFeatures;
    if (labelsFilter.size() > 0) {
      validFeatures = filterFeaturesByAllLabels(this.getFeatures(), labelsFilter);
      validFeaturesMap = getFeaturesRefToFeaturesMap(validFeatures);
      return validFeaturesMap;
    }
    validFeaturesMap = getFeaturesRefToFeaturesMap(List.copyOf(this.getFeatures()));
    return validFeaturesMap;
  }

  /**
   * Returns map for accessing features using their respective feature reference.
   *
   * @param features List of features to insert to map.
   * @return Map of featureRef:feature.
   */
  private Map<String, FeatureV2> getFeaturesRefToFeaturesMap(List<FeatureV2> features) {
    Map<String, FeatureV2> validFeaturesMap = new HashMap<>();
    for (FeatureV2 feature : features) {
      ServingAPIProto.FeatureReferenceV2 featureRef =
          ServingAPIProto.FeatureReferenceV2.newBuilder()
              .setFeatureTable(this.getName())
              .setName(feature.getName())
              .build();
      validFeaturesMap.put(getFeatureStringRef(featureRef), feature);
    }
    return validFeaturesMap;
  }

  /**
   * Returns a list of Features if FeatureTable's Feature contains all labels in labelsFilter
   *
   * @param features features
   * @param labelsFilter contain labels that should be attached to FeatureTable's features
   * @return List of Features
   */
  public static List<FeatureV2> filterFeaturesByAllLabels(
      Set<FeatureV2> features, Map<String, String> labelsFilter) {
    List<FeatureV2> validFeatures =
        features.stream()
            .filter(feature -> feature.hasAllLabels(labelsFilter))
            .collect(Collectors.toList());

    return validFeatures;
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

  public void delete() {
    this.setDeleted(true);
    this.setRevision(0);
  }

  public String protoHash() {
    List<String> sortedEntities =
        this.getEntities().stream().map(EntityV2::getName).sorted().collect(Collectors.toList());

    List<FeatureSpecV2> sortedFeatureSpecs =
        this.getFeatures().stream()
            .sorted(Comparator.comparing(FeatureV2::getName))
            .map(FeatureV2::toProto)
            .collect(Collectors.toList());

    DataSourceProto.DataSource streamSource = DataSourceProto.DataSource.getDefaultInstance();
    if (getStreamSource() != null) {
      streamSource = getStreamSource().toProto();
    }

    FeatureTableSpec featureTableSpec =
        FeatureTableSpec.newBuilder()
            .addAllEntities(sortedEntities)
            .addAllFeatures(sortedFeatureSpecs)
            .setBatchSource(getBatchSource().toProto())
            .setStreamSource(streamSource)
            .setMaxAge(Duration.newBuilder().setSeconds(getMaxAgeSecs()).build())
            .build();
    return Hashing.murmur3_32().hashBytes(featureTableSpec.toByteArray()).toString();
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
        && getFeatures().equals(other.getFeatures())
        && getEntities().equals(other.getEntities())
        && getMaxAgeSecs() == other.getMaxAgeSecs()
        && Optional.ofNullable(getBatchSource()).equals(Optional.ofNullable(other.getBatchSource()))
        && Optional.ofNullable(getStreamSource())
            .equals(Optional.ofNullable(other.getStreamSource()));
  }
}
