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

import feast.core.dao.EntityRepository;
import feast.core.util.TypeConversion;
import feast.proto.core.FeatureTableProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;

import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import lombok.Getter;

@Getter
@Entity
@Table(
    name = "feature_table",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project_name"}))
public class FeatureTable extends AbstractTimestampEntity {

  @Id @GeneratedValue private long id;

  // Name of Feature Table
  @Column(name = "name", nullable = false)
  private String name;

  // Name of the Project that this FeatureTable belongs to
  @Column(name = "project_name", nullable = false)
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

  // Streaming Feature Source used to obtain data for features from a stream
  @Column(name = "stream_source_id")
  private FeatureSource streamSource;

  // Batched Feature Source used to obtain data for features from a batch of data
  @Column(name = "batch_source_id")
  private FeatureSource batchSource;

  // Autoincrementing version no. of this Feature Table.
  // Autoincrements every update made to the feature set.
  @Column(name = "revision", nullable = false)
  private int revision;

  private FeatureTable(
      String name,
      String projectName,
      Set<FeatureV2> features,
      Set<EntityV2> entities,
      String labelsJSON,
      long maxAgeSecs,
      FeatureSource streamSource,
      FeatureSource batchSource) {
    this.name = name;
    this.project = new Project(projectName);
    this.features = features;
    this.entities = entities;
    this.labelsJSON = labelsJSON;
    this.maxAgeSecs = maxAgeSecs;
    this.streamSource = streamSource;
    this.batchSource = batchSource;
    this.revision = 0;
  }

  /**
   * Construct Feature from Protobuf spec representation in the given project
   * with entities registered in entity repository.
   *
   * @param projectName the name of the project that the constructed Feature table belongs.
   * @param spec the Protobuf spec to contruct the Feature from.
   * @param entityRepo {@link EntityRepository} used to resolve entity names.
   * @throws IllegalArgumentException if the Protobuf spec provided is invalid.
   * @return constructed Feature from the given Protobuf spec.
   */
  public static FeatureTable fromProto(
      String projectName, FeatureTableProto.FeatureTableSpec spec, EntityRepository entityRepo) {
    // Convert types of fields into storable format
    Set<FeatureV2> features =
        spec.getFeaturesList().stream().map(FeatureV2::fromProto).collect(Collectors.toSet());
    FeatureSource streamSource = FeatureSource.fromProto(spec.getStreamSource());
    FeatureSource batchSource = FeatureSource.fromProto(spec.getBatchSource());
    Set<EntityV2> entities = FeatureTable.resolveEntities(projectName, entityRepo, spec.getEntitiesList());
    String labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
  
    return new FeatureTable(
        spec.getName(),
        projectName,
        features,
        entities,
        labelsJSON,
        spec.getMaxAge().getSeconds(),
        streamSource,
        batchSource);
  }

  /**
   * Update the FeatureTable from the given Protobuf representation.
   *
   * @param spec the Protobuf spec to update the FeatureTable from.
   * @throws IllegalArgumentException if the update will make prohibited changes.
  */
  public void updateFromProto(FeatureTableProto.FeatureTableSpec spec) {
    // Check for prohibited changes made in spec
    if(!getName().equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format("Updating the name of a registered FeatureTable is not allowed: %s to %s",
            getName(), spec.getName()));
    }
    List<String> entityNames = getEntities().stream().map(EntityV2::getName).collect(Collectors.toList());
    if(!entityNames.equals(spec.getEntitiesList())) {
      throw new IllegalArgumentException(
          String.format("Updating the entities of a registered FeatureTable is not allowed: %s to %s",
            entityNames, spec.getEntitiesList()));
    }
   
    // Convert types of fields & update FeatureTable based on spec
    this.maxAgeSecs = spec.getMaxAge().getSeconds();
    this.labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
    this.batchSource = FeatureSource.fromProto(spec.getBatchSource());
    this.streamSource = FeatureSource.fromProto(spec.getStreamSource());
    // Bump revision no.
    this.revision ++;
  }
  
  /** Convert this Feature Table to its Protobuf representation */
  public FeatureTableProto.FeatureTable toProto() {
    // Convert field types to Protobuf compatible types
    Timestamp creationTime = TypeConversion.convertTimestamp(getCreated());
    Timestamp updatedTime = TypeConversion.convertTimestamp(getLastUpdated());
    List<FeatureSpecV2> featureSpecs = getFeatures().stream().map(FeatureV2::toProto).collect(Collectors.toList());
    List<String> entityNames = getEntities().stream().map(EntityV2::getName).collect(Collectors.toList());
    Map<String, String> labels = TypeConversion.convertJsonStringToMap(getLabelsJSON());

    return FeatureTableProto.FeatureTable.newBuilder()
      .setMeta(FeatureTableProto.FeatureTableMeta.newBuilder()
          .setRevision(getRevision())
          .setCreatedTimestamp(creationTime)
          .setLastUpdatedTimestamp(updatedTime)
          .build())
      .setSpec(FeatureTableProto.FeatureTableSpec.newBuilder()
          .setName(getName())
          .setMaxAge(Duration.newBuilder().setSeconds(getMaxAgeSecs()).build())
          .addAllEntities(entityNames)
          .addAllFeatures(featureSpecs)
          .putAllLabels(labels)
          .build())
      .build();
  }
  
  /** Use given entity repository to resolve entity names to entity native objects */
  private static Set<EntityV2> resolveEntities(String projectName, EntityRepository repo, Collection<String> names) {
    return names.stream()
            .map(
                entityName -> {
                  EntityV2 entity =
                      repo.findEntityByNameAndProject_Name(entityName, projectName);
                  if (entity == null) {
                    throw new IllegalArgumentException(
                        String.format(
                            "Feature Table refers to no existent Entity with name %s in project %s.",
                            entityName, projectName));
                  }
                  return entity;
                })
            .collect(Collectors.toSet());
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(
        getName(), getProject(), getFeatures(), getEntities(), getMaxAgeSecs(),
        getBatchSource(), getStreamSource());
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
    
    return getName().equals(other.getName()) &&
        getProject().equals(other.getProject()) &&
        getLabelsJSON().equals(other.getLabelsJSON()) &&
        getMaxAgeSecs() == getMaxAgeSecs() &&
        getBatchSource().equals(getBatchSource()) &&
        getStreamSource().equals(getStreamSource());
  }
  
}
